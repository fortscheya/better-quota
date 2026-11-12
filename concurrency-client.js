// Concurrency edits save independently of the autoreset/settings form.
// Keep unconfirmed drafts through refreshes and serialize partial patches.
const concurrencyDrafts=new Map(),concurrencyScheduled=new Map(),concurrencyTimers=new Map();
let concurrencyQueue=Promise.resolve(),concurrencyLatest=Promise.resolve();
function concurrencyFeedback(message,state){const feedback=el('concurrency-feedback');feedback.textContent=message;feedback.dataset.state=state}
function concurrencyInput(id){return [...document.querySelectorAll('.concurrency-limit')].find(input=>input.dataset.account===id)}
function refreshConcurrencyCells(status,ids){
 current=status;
 for(const id of ids){
  const account=(status.dashboard_accounts||[]).find(a=>a.credential.id===id),input=concurrencyInput(id);
  if(!account||!input)continue;
  const cell=input.closest('td'),updated=renderAccount(account).querySelector('.concurrency-cell');
  input.dataset.original=String(account.concurrency_limit);
  if(!concurrencyDrafts.has(id))input.value=account.concurrency_limit;
  cell.querySelector('.concurrency-note').textContent=concurrencyDrafts.has(id)?'未保存':'';
  for(const selector of ['.slot-bar','.slot-count'])cell.querySelector(selector).replaceWith(updated.querySelector(selector));
 }
}
function persistConcurrency(ids){
 const values={},drafts=new Map();
 for(const id of ids){
  clearTimeout(concurrencyTimers.get(id));concurrencyTimers.delete(id);
  if(!concurrencyDrafts.has(id))continue;
  const raw=concurrencyDrafts.get(id),value=Number(raw);
  if(!String(raw).trim()||!Number.isInteger(value)||value<1||value>10000){
   concurrencyFeedback('并发上限必须是 1 到 10000 的整数，修改尚未保存。','error');
   return Promise.reject(Error('并发上限必须是 1 到 10000 的整数'));
  }
  if(concurrencyScheduled.get(id)===raw)continue;
  drafts.set(id,raw);values[id]=value;
 }
 if(!drafts.size)return concurrencyLatest;
 for(const [id,raw]of drafts)concurrencyScheduled.set(id,raw);
 const task=concurrencyQueue.then(async()=>{
  concurrencyFeedback('正在保存并发上限…','saving');
  try{
   await api('/settings',{active_overrides:values});
   const status=await api('/status');
   // A successful HTTP reply is insufficient: verify the effective gate limit.
   for(const [id,value]of Object.entries(values)){
    const account=(status.dashboard_accounts||[]).find(a=>a.credential.id===id);
    if(!account||account.concurrency_limit!==value)throw Error('并发上限尚未确认生效，请重试。');
   }
   for(const [id,raw]of drafts)if(concurrencyDrafts.get(id)===raw)concurrencyDrafts.delete(id);
   refreshConcurrencyCells(status,[...drafts.keys()]);
   if(!concurrencyDrafts.size)concurrencyFeedback('并发上限已保存并生效。','saved');
  }catch(error){
   concurrencyFeedback('并发上限保存失败，修改尚未确认生效；请重试。','error');
   for(const id of drafts.keys()){const input=concurrencyInput(id);if(input)input.closest('td').querySelector('.concurrency-note').textContent='未保存'}
   throw error;
  }finally{
   for(const [id,raw]of drafts)if(concurrencyScheduled.get(id)===raw)concurrencyScheduled.delete(id);
  }
 });
 concurrencyLatest=task;concurrencyQueue=task.catch(()=>{});
 return task;
}
function editConcurrency(input){
 const id=input.dataset.account;
 concurrencyDrafts.set(id,input.value);
 input.closest('td').querySelector('.concurrency-note').textContent='未保存';
 concurrencyFeedback('并发上限待保存…','pending');
 clearTimeout(concurrencyTimers.get(id));
 concurrencyTimers.set(id,setTimeout(()=>persistConcurrency([id]).catch(()=>{}),300));
}
async function flushConcurrency(){
 while(concurrencyDrafts.size)await persistConcurrency([...concurrencyDrafts.keys()]);
 await concurrencyLatest;
}
async function setConcurrencyFor(rows){
 const value=Number(el('batch-limit').value);
 if(!Number.isInteger(value)||value<1||value>10000){concurrencyFeedback('批量并发上限必须是 1 到 10000 的整数。','error');return false}
 const inputs=rows.map(row=>row.querySelector('.concurrency-limit')).filter(Boolean);
 if(!inputs.length){concurrencyFeedback('请先选择需要设置的凭证。','error');return false}
 for(const input of inputs){input.value=value;concurrencyDrafts.set(input.dataset.account,input.value)}
 try{await persistConcurrency(inputs.map(input=>input.dataset.account));return true}catch{return false}
}
