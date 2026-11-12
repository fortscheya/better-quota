package main

/*
#include <stdint.h>
#include <stdlib.h>

typedef struct {
	void* ptr;
	size_t len;
} cliproxy_buffer;

typedef int (*cliproxy_host_call_fn)(void*, const char*, const uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_host_free_fn)(void*, size_t);

typedef struct {
	uint32_t abi_version;
	void* host_ctx;
	cliproxy_host_call_fn call;
	cliproxy_host_free_fn free_buffer;
} cliproxy_host_api;

typedef int (*cliproxy_plugin_call_fn)(char*, uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_plugin_free_fn)(void*, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);

typedef struct {
	uint32_t abi_version;
	cliproxy_plugin_call_fn call;
	cliproxy_plugin_free_fn free_buffer;
	cliproxy_plugin_shutdown_fn shutdown;
} cliproxy_plugin_api;

extern int cliproxyPluginCall(char*, uint8_t*, size_t, cliproxy_buffer*);
extern void cliproxyPluginFree(void*, size_t);
extern void cliproxyPluginShutdown(void);

static const cliproxy_host_api* stored_host;

static void store_host_api(const cliproxy_host_api* host) {
	stored_host = host;
}

static int call_host_api(const char* method, const uint8_t* request, size_t request_len, cliproxy_buffer* response) {
	if (stored_host == NULL || stored_host->call == NULL) {
		return 1;
	}
	return stored_host->call(stored_host->host_ctx, method, request, request_len, response);
}

static void free_host_buffer(void* ptr, size_t len) {
	if (stored_host != NULL && stored_host->free_buffer != NULL && ptr != NULL) {
		stored_host->free_buffer(ptr, len);
	}
}
*/
import "C"

import (
	"encoding/json"
	"errors"

	"unsafe"
)

const abiVersion uint32 = 1
const maxRPCRequestBytes = 16 << 20

// Request hooks and the stream observer are intentionally transparent to the
// request/response bodies. CPA may include a large prompt or stream payload in
// their RPC envelope, so the generic ABI size guard must not turn a no-op
// interceptor into a failed host callback. Other methods keep the bounded
// request limit. HookRequest omits Body, so JSON decoding skips that field.
func allowsOversizedRPCRequest(method string) bool {
	switch method {
	case "request.intercept_before", "request.intercept_after", "response.intercept_stream_chunk", "websocket.response_event":
		return true
	default:
		return false
	}
}

type envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *envelopeError  `json:"error,omitempty"`
}

type envelopeError struct {
	Code              string `json:"code"`
	Message           string `json:"message"`
	HTTPStatus        int    `json:"http_status,omitempty"`
	RetryAfterSeconds int    `json:"retry_after_seconds,omitempty"`
}

func main() {}

//export cliproxy_plugin_init
func cliproxy_plugin_init(host *C.cliproxy_host_api, plugin *C.cliproxy_plugin_api) C.int {
	if plugin == nil || host == nil || uint32(host.abi_version) != abiVersion || host.call == nil || host.free_buffer == nil {
		return 1
	}
	C.store_host_api(host)
	plugin.abi_version = C.uint32_t(abiVersion)
	plugin.call = C.cliproxy_plugin_call_fn(C.cliproxyPluginCall)
	plugin.free_buffer = C.cliproxy_plugin_free_fn(C.cliproxyPluginFree)
	plugin.shutdown = C.cliproxy_plugin_shutdown_fn(C.cliproxyPluginShutdown)
	return 0
}

//export cliproxyPluginCall
func cliproxyPluginCall(method *C.char, request *C.uint8_t, requestLen C.size_t, response *C.cliproxy_buffer) C.int {
	if response != nil {
		response.ptr = nil
		response.len = 0
	}
	if method == nil {
		writeResponse(response, errorEnvelope("invalid_method", "method is required"))
		return 1
	}
	methodName := C.GoString(method)
	if request == nil && requestLen > 0 {
		writeResponse(response, errorEnvelope("invalid_request", "request buffer is invalid or too large"))
		return 1
	}
	if requestLen > maxRPCRequestBytes && !allowsOversizedRPCRequest(methodName) {
		writeResponse(response, errorEnvelope("invalid_request", "request buffer is invalid or too large"))
		return 1
	}
	// Stream chunks are an observe-only hook. Once the envelope is too large to
	// copy through the generic Go ABI path, acknowledge it without decoding it;
	// CPA keeps its current Body because this response contains no replacement or
	// DropChunk instruction. This is the true pass-through path for large and
	// non-Codex streams.
	if requestLen > maxRPCRequestBytes && methodName == "response.intercept_stream_chunk" {
		raw, errHandle := ok(struct{}{})
		if errHandle != nil {
			writeResponse(response, errorEnvelope("plugin_error", errHandle.Error()))
			return 1
		}
		writeResponse(response, raw)
		return 0
	}
	// Borrow the host buffer only for this synchronous call. This avoids both a
	// second full-body copy and C.GoBytes' signed 32-bit length conversion. JSON
	// decoding owns any fields retained by the runtime; no input slice escapes.
	if requestLen > C.size_t(^uint(0)>>1) {
		writeResponse(response, errorEnvelope("invalid_request", "request buffer length is out of range"))
		return 1
	}
	raw, errHandle := handleMethod(methodName, unsafe.Slice((*byte)(unsafe.Pointer(request)), int(requestLen)))
	if errHandle != nil {
		writeResponse(response, errorEnvelope("plugin_error", errHandle.Error()))
		return 1
	}
	writeResponse(response, raw)
	_ = request
	_ = requestLen
	return 0
}

//export cliproxyPluginFree
func cliproxyPluginFree(ptr unsafe.Pointer, len C.size_t) {
	if ptr != nil {
		C.free(ptr)
	}
	_ = len
}

//export cliproxyPluginShutdown
func cliproxyPluginShutdown() { runtimeState.stop() }

func okEnvelopeJSON(result string) ([]byte, error) {
	return json.Marshal(envelope{OK: true, Result: json.RawMessage(result)})
}

func errorEnvelope(code, message string) []byte {
	raw, _ := json.Marshal(envelope{OK: false, Error: &envelopeError{Code: code, Message: message}})
	return raw
}

func writeResponse(response *C.cliproxy_buffer, raw []byte) {
	if response == nil || len(raw) == 0 {
		return
	}
	ptr := C.CBytes(raw)
	if ptr == nil {
		return
	}
	response.ptr = ptr
	response.len = C.size_t(len(raw))
}

func nativeHostCall(method string, payload []byte) ([]byte, error) {
	cMethod := C.CString(method)
	defer C.free(unsafe.Pointer(cMethod))
	var response C.cliproxy_buffer
	var req *C.uint8_t
	if len(payload) > 0 {
		req = (*C.uint8_t)(C.CBytes(payload))
		defer C.free(unsafe.Pointer(req))
	}
	rc := C.call_host_api(cMethod, req, C.size_t(len(payload)), &response)
	if response.ptr == nil {
		return nil, errors.New("host callback unavailable")
	}
	defer C.free_host_buffer(response.ptr, response.len)
	raw := C.GoBytes(response.ptr, C.int(response.len))
	if rc != 0 {
		return nil, errors.New("host callback failed")
	}
	return raw, nil
}
