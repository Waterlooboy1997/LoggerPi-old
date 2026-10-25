// Run with node tests/test_dashboard.js; exercises the live editor against a small DOM stub.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const elements = new Map();
function element(id) {
  if (!elements.has(id)) elements.set(id, {
    value:'', checked:false, textContent:'', innerHTML:'', children:[], handlers:{},
    addEventListener(name, handler) { this.handlers[name]=handler; },
    querySelectorAll() { return []; }, replaceChildren() { this.children=[]; },
    appendChild(child) { this.children.push(child); }
  });
  return elements.get(id);
}
const context = vm.createContext({
  document: {getElementById:element, querySelector:()=>({content:'token'}), createElement:()=>({})},
  fetch:()=>new Promise(()=>{}), setInterval:()=>{}, console, URLSearchParams, Date,
});
const root = path.join(__dirname, '..');
vm.runInContext(fs.readFileSync(path.join(root,'static/dashboard.js'),'utf8'),context);
const run = code=>vm.runInContext(code,context);
const port = {port:310,site:'HUL',listening:true,clients:[],reject_count:0,duplicate_ip_count:0};
context.port = port;
run('latestPorts=[port]; selected=310; renderPorts(); renderDetail(port)');
assert.match(element('ports').innerHTML, /<td>310<\/td><td>HUL<\/td><td>Yes<\/td>/);
assert.equal(element('site-name').value,'HUL');
element('site-name').value='Draft';
element('site-name').handlers.input();
run('renderDetail(port)');
assert.equal(element('site-name').value,'Draft', 'polling preserves unsaved input');
element('site-cancel').handlers.click();
assert.equal(element('site-name').value,'HUL');
element('search').value='hul'; run('renderPorts()');
assert.match(element('ports').innerHTML, /HUL/);
port.site='<script>&'; element('search').value=''; run('renderPorts()');
assert.match(element('ports').innerHTML, /&lt;script&gt;&amp;/);
assert.doesNotMatch(element('ports').innerHTML, /<script>/);
const template = fs.readFileSync(path.join(root,'templates/dashboard.html'),'utf8');
assert.match(template, /<th>Port<\/th><th>Site<\/th><th>Listening<\/th>/);
// Client roles from the PakBus check.
port.clients=[{peer:'120.157.206.45:1261',role:'logger',role_detail:'PakBus node 353',checked_at:new Date(Date.now()-65000).toISOString()}];
run('renderPorts(); renderDetail(port)');
assert.match(element('ports').innerHTML, /120\.157\.206\.45:1261 \(logger\)<\/td>/);
assert.match(element('detail').innerHTML, /Role: logger · PakBus node 353 · answered 00:01:0\d ago/);
port.clients=[{peer:'10.0.0.9:1',role:null,role_detail:'did not answer a PakBus Hello sent to node 353'}];
run('renderDetail(port)');
assert.match(element('detail').innerHTML, /Role: unknown · did not answer a PakBus Hello sent to node 353/);
port.clients=[{peer:'10.0.0.9:1',role:null,role_detail:''}];
run('renderDetail(port)');
assert.doesNotMatch(element('detail').innerHTML, /Role:/);
port.clients=[];
(async()=>{
  let request;
  context.fetch=async(url, options)=>{request={url,options};return {ok:true,json:async()=>({site:'Saved'})}};
  element('site-name').value='Saved'; element('site-name').handlers.input();
  await element('site-form').handlers.submit({preventDefault(){}});
  assert.equal(request.url,'/api/ports/310/site');
  assert.equal(request.options.headers['X-Control-Token'],'token');
  assert.equal(JSON.parse(request.options.body).site,'Saved');
  assert.equal(element('site-message').textContent,'Site saved.');
  assert.equal(port.site,'Saved');
  context.fetch=async()=>({ok:false,status:500,text:async()=> 'Cannot save'});
  element('site-name').value='Keep draft'; element('site-name').handlers.input();
  await element('site-form').handlers.submit({preventDefault(){}});
  assert.match(element('site-message').textContent,/Save failed/);
  run('renderDetail(port)');
  assert.equal(element('site-name').value,'Keep draft');
  assert.equal(element('site-save').disabled,false);
  context.otherPort={...port,port:361,site:'Free'};
  run('renderDetail(otherPort)');
  assert.equal(element('site-name').value,'Free');
  console.log('Dashboard site editor checks passed.');
})().catch(error=>{console.error(error);process.exitCode=1});
