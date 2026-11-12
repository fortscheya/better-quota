#define _POSIX_C_SOURCE 200809L
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <dlfcn.h>
#include <stdatomic.h>

typedef struct {void *ptr; size_t len;} buffer;
typedef struct {uint32_t abi_version; void *host_ctx; int (*call)(void*,const char*,const uint8_t*,size_t,buffer*); void (*free_buffer)(void*,size_t);} host_api;
typedef struct {uint32_t abi_version; int (*call)(char*,uint8_t*,size_t,buffer*); void (*free_buffer)(void*,size_t); void (*shutdown)(void);} plugin_api;
static atomic_int unexpected;
static int host_call(void *ctx,const char *method,const uint8_t *req,size_t n,buffer *out){
 (void)ctx;(void)req;(void)n;
 const char *reply="{\"ok\":true,\"result\":{\"files\":[]}}";
 if(strcmp(method,"host.auth.list")!=0){atomic_fetch_add(&unexpected,1);reply="{\"ok\":false,\"error\":{\"code\":\"mock_only\",\"message\":\"no external calls allowed\"}}";}
 out->len=strlen(reply);out->ptr=malloc(out->len);memcpy(out->ptr,reply,out->len);return 0;
}
static void release(void *p,size_t n){(void)n;free(p);}
static void call(plugin_api *api,const char *method,const char *json,const char *expect){
 buffer out={0};int rc=api->call((char*)method,(uint8_t*)json,strlen(json),&out);
 char *text=calloc(out.len+1,1);if(out.ptr)memcpy(text,out.ptr,out.len);
 if(rc!=0||!out.ptr||!strstr(text,expect)){fprintf(stderr,"ABI %s failed rc=%d body=%s\n",method,rc,text);exit(1);}
 api->free_buffer(out.ptr,out.len);free(text);
}
static void call_large(plugin_api *api,const char *method,const char *tail,int expected_rc,const char *expect){
 const size_t body_len=17u<<20;
 const char *prefix="{\"RequestID\":\"large-request\",\"Body\":\"";
 size_t prefix_len=strlen(prefix),tail_len=strlen(tail),n=prefix_len+body_len+tail_len;
 char *json=malloc(n+1);if(!json)exit(1);
 memcpy(json,prefix,prefix_len);memset(json+prefix_len,'x',body_len);memcpy(json+prefix_len+body_len,tail,tail_len);json[n]=0;
 buffer out={0};int rc=api->call((char*)method,(uint8_t*)json,n,&out);
 char *text=calloc(out.len+1,1);if(out.ptr)memcpy(text,out.ptr,out.len);
 if(rc!=expected_rc||!out.ptr||!strstr(text,expect)){fprintf(stderr,"large ABI %s failed rc=%d body=%s\n",method,rc,text);exit(1);}
 api->free_buffer(out.ptr,out.len);free(text);free(json);
}
static void reject_invalid_buffer(plugin_api *api,uint8_t *ptr,size_t n){
 buffer out={0};int rc=api->call("request.intercept_before",ptr,n,&out);
 char *text=calloc(out.len+1,1);if(out.ptr)memcpy(text,out.ptr,out.len);
 if(rc!=1||!out.ptr||!strstr(text,"invalid_request")){fprintf(stderr,"invalid ABI buffer accepted\n");exit(1);}
 api->free_buffer(out.ptr,out.len);free(text);
}
int main(int argc,char **argv){
 if(argc!=2)return 2;
 void *lib=dlopen(argv[1],RTLD_NOW|RTLD_LOCAL);if(!lib){fprintf(stderr,"%s\n",dlerror());return 1;}
 int (*init)(host_api*,plugin_api*)=dlsym(lib,"cliproxy_plugin_init");if(!init)return 1;
 host_api host={1,NULL,host_call,release};plugin_api plugin={0};
 if(init(&host,&plugin)!=0||plugin.abi_version!=1||!plugin.call||!plugin.free_buffer||!plugin.shutdown)return 1;
 call(&plugin,"plugin.register","{}","\"request_lifecycle_plugin\":true");
 reject_invalid_buffer(&plugin,NULL,1);
 uint8_t dummy=0;reject_invalid_buffer(&plugin,&dummy,SIZE_MAX);
 call_large(&plugin,"request.intercept_before","\"}",0,"large-request");
 call_large(&plugin,"request.intercept_after","\",\"ToFormat\":\"zcode\",\"Metadata\":{\"selected_auth_id\":\"zcode-auth\"}}",0,"\"Terminate\":false");
 call_large(&plugin,"response.intercept_stream_chunk","\"}",0,"\"result\":{}");
 call_large(&plugin,"websocket.response_event","\"}",0,"\"result\":{}");
 call_large(&plugin,"management.handle","\"}",1,"invalid_request");
 call(&plugin,"request.complete","{\"RequestID\":\"large-request\"}","\"ok\":true");
 call(&plugin,"request.intercept_before","{\"RequestID\":\"abi-request\",\"Headers\":{\"X-Better-Quota-Execution\":[\"forged\"]}}","abi-request");
 call(&plugin,"scheduler.pick","{\"Provider\":\"codex\",\"Options\":{\"Headers\":{\"X-Better-Quota-Execution\":[\"abi-request\"]}},\"Candidates\":[{\"ID\":\"fake\",\"Provider\":\"codex\"}]}","quota_unavailable");
 call(&plugin,"request.intercept_after","{\"RequestID\":\"abi-request\",\"ToFormat\":\"codex\",\"Metadata\":{\"selected_auth_id\":\"fake\"}}","\"Terminate\":true");
 call(&plugin,"response.intercept_stream_chunk","{\"RequestID\":\"abi-request\",\"Body\":\"ZGF0YTo=\"}","\"ok\":true");
 call(&plugin,"websocket.response_event","{\"RequestID\":\"abi-request\",\"EventType\":\"response.completed\"}","\"ok\":true");
 call(&plugin,"request.complete","{\"RequestID\":\"abi-request\",\"Outcome\":\"canceled\"}","\"ok\":true");
 call(&plugin,"management.register","{\"BasePath\":\"/v0/management\",\"ResourceBasePath\":\"/v0/resource/plugins/better-quota\"}","/v0/management/plugins/better-quota/status");
 call(&plugin,"management.handle","{\"Method\":\"GET\",\"Path\":\"/v0/resource/plugins/better-quota/status\"}","\"StatusCode\":404");
 call(&plugin,"quota.identifier","{}","better-quota");
 call(&plugin,"plugin.quiesce","{}","\"ok\":true");plugin.shutdown();
 if(atomic_load(&unexpected)!=0){fprintf(stderr,"unexpected host callback\n");return 1;}
 /* Go c-shared runtimes remain resident, matching CPA's logical shutdown policy. */
 puts("PASS: C ABI v1 / schema v6, lifecycle, rejection, management boundary, no external calls");return 0;
}
