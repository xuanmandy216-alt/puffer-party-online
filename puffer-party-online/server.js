'use strict';
const express=require('express');
const http=require('http');
const {Server}=require('socket.io');
const path=require('path');
const crypto=require('crypto');
const app=express();const server=http.createServer(app);const io=new Server(server,{pingTimeout:20000});
app.use(express.static(path.join(__dirname,'public')));
app.get('/health',(req,res)=>res.json({ok:true,rooms:rooms.size}));
const PORT=process.env.PORT||3000;
const rooms=new Map();
const questions={
  everyday:['说出一种水果','说出一种蔬菜','说出一个中国城市','说出一个国家','说出一种动物','说出一种鸟','说出一种饮料','说出一道家常菜','说出一种运动','说出一个职业','说出一个交通工具','说出一种颜色','说出一个节日','说出一个家用电器','说出一种乐器','说出一个手机品牌','说出一个天气现象','说出一种花','说出一个大学专业','说出一种甜品','说出一种海洋生物','说出一个旅游景点','说出一个身体部位','说出一个四字成语','说出一种零食','说出一个社交软件','说出一种宠物','说出一个服装品牌','说出一个姓氏','说出一种早餐'],
  funny:['说出一个迟到的借口','说出一个不想上课的理由','说出一个分手理由','说出一件社死的事情','说出一个奇怪的网名','说出一句土味情话','说出一种奇葩超能力','说出一个离谱的请假理由','说出一个不该在约会上说的话','说出一个让人瞬间下头的行为','说出一个半夜会做的事情','说出一个不适合当密码的词','说出一个让朋友无语的行为','说出一个你绝对不想收到的礼物','说出一个适合给群聊起的名字','说出一个不想接电话的借口','说出一个派对上可能发生的尴尬场面','说出一个奇怪的外卖备注','说出一个最没用的发明','说出一个假装很忙的方法'],
  friends:['说出一个适合朋友一起去的地方','说出一个朋友间常用的称呼','说出一种聚会游戏','说出一个友情变淡的原因','说出一个值得庆祝的小事','说出一种朋友的优点','说出一种朋友的缺点','说出一个真心话题目','说出一个大冒险任务','说出一个旅行必带的东西','说出一个适合拍合照的姿势','说出一个朋友之间的共同回忆','说出一个让你感动的小举动','说出一个朋友可能有的隐藏技能','说出一个聚会上的惩罚','说出一种适合深夜聊天的话题','说出一个适合朋友的礼物','说出一个旅行时会吵架的原因','说出一个朋友之间的内部梗','说出一个朋友迟到时的反应']
};
const clean=s=>String(s||'').trim().slice(0,24);
const norm=s=>clean(s).toLocaleLowerCase('zh-CN').replace(/[\s\p{P}\p{S}]/gu,'');
const code=()=>{let c;do{c=crypto.randomBytes(3).toString('hex').toUpperCase()}while(rooms.has(c));return c};
const emitError=(socket,msg)=>socket.emit('notice',{message:msg});
function publicState(room){return {code:room.code,hostId:room.hostId,phase:room.phase,players:room.players.map(({id,name,lives,connected})=>({id,name,lives,connected})),settings:room.settings,question:room.question,turnId:room.turnId,deadline:room.deadline,used:room.used.map(x=>({answer:x.answer,name:x.name})),round:room.round,lastEvent:room.lastEvent,winnerId:room.winnerId};}
function broadcast(room){io.to(room.code).emit('state',publicState(room))}
function stopTimer(room){if(room.timer){clearTimeout(room.timer);room.timer=null}}
function randomQuestion(room){const pool=room.settings.category==='all'?Object.values(questions).flat():questions[room.settings.category]||questions.everyday;let q=pool[Math.floor(Math.random()*pool.length)];if(pool.length>1&&q===room.question)q=pool[(pool.indexOf(q)+1)%pool.length];return q}
function alive(room){return room.players.filter(p=>p.lives>0)}
function nextTurn(room,afterId){const a=alive(room);if(a.length<2){finish(room);return}let i=a.findIndex(p=>p.id===afterId);room.turnId=a[(i+1+a.length)%a.length].id}
function finish(room){stopTimer(room);room.phase='finished';room.deadline=null;room.winnerId=alive(room)[0]?.id||null;room.lastEvent={type:'finish',name:alive(room)[0]?.name||'无人'};broadcast(room)}
function startRound(room){stopTimer(room);if(alive(room).length<2){finish(room);return}room.phase='playing';room.round++;room.question=randomQuestion(room);room.used=[];room.turnId=alive(room)[Math.floor(Math.random()*alive(room).length)].id;room.winnerId=null;room.lastEvent={type:'round',round:room.round};const min=room.settings.minSeconds,max=room.settings.maxSeconds;room.deadline=Date.now()+(min+Math.random()*(max-min))*1000;room.timer=setTimeout(()=>explode(room),room.deadline-Date.now());broadcast(room)}
function explode(room){if(room.phase!=='playing')return;stopTimer(room);const p=room.players.find(p=>p.id===room.turnId);if(p)p.lives=Math.max(0,p.lives-1);room.phase='intermission';room.deadline=null;room.lastEvent={type:'explode',name:p?.name||'玩家',lives:p?.lives??0};broadcast(room);if(alive(room).length<2){room.timer=setTimeout(()=>finish(room),2200)}else{room.timer=setTimeout(()=>startRound(room),3500)}}
function leave(socket){const c=socket.data.roomCode;const room=rooms.get(c);if(!room)return;const i=room.players.findIndex(p=>p.id===socket.id);if(i<0)return;const p=room.players[i];room.players.splice(i,1);socket.leave(c);socket.data.roomCode=null;if(!room.players.length){stopTimer(room);rooms.delete(c);return}if(room.hostId===socket.id)room.hostId=room.players[0].id;if(room.phase==='playing'&&room.turnId===socket.id){if(alive(room).length<2)finish(room);else{nextTurn(room,socket.id);broadcast(room)}}else if(room.phase!=='waiting'&&alive(room).length<2)finish(room);else broadcast(room)}
io.on('connection',socket=>{
  socket.on('createRoom',(data={},ack=()=>{})=>{leave(socket);const name=clean(data.name);if(!name)return ack({ok:false,error:'请输入昵称'});const c=code();const room={code:c,hostId:socket.id,players:[{id:socket.id,name,lives:3,connected:true}],phase:'waiting',settings:{lives:3,category:'all',minSeconds:12,maxSeconds:28},question:'',turnId:null,deadline:null,used:[],round:0,lastEvent:null,winnerId:null,timer:null};rooms.set(c,room);socket.join(c);socket.data.roomCode=c;ack({ok:true,code:c,id:socket.id});broadcast(room)});
  socket.on('joinRoom',(data={},ack=()=>{})=>{const c=String(data.code||'').trim().toUpperCase();const room=rooms.get(c);const name=clean(data.name);if(!room)return ack({ok:false,error:'找不到房间，请检查房间码'});if(room.phase!=='waiting')return ack({ok:false,error:'游戏已开始，请等待下一局'});if(room.players.length>=10)return ack({ok:false,error:'房间已满（最多10人）'});if(!name)return ack({ok:false,error:'请输入昵称'});leave(socket);room.players.push({id:socket.id,name,lives:room.settings.lives,connected:true});socket.join(c);socket.data.roomCode=c;ack({ok:true,code:c,id:socket.id});broadcast(room)});
  socket.on('settings',(data={})=>{const room=rooms.get(socket.data.roomCode);if(!room||room.hostId!==socket.id||room.phase!=='waiting')return;const lives=Number(data.lives),min=Number(data.minSeconds),max=Number(data.maxSeconds);if(![1,2,3,4,5].includes(lives)||!Number.isInteger(min)||!Number.isInteger(max)||min<5||max>90||max<min+3||!['all','everyday','funny','friends'].includes(data.category))return emitError(socket,'设置无效');room.settings={lives,minSeconds:min,maxSeconds:max,category:data.category};room.players.forEach(p=>p.lives=lives);broadcast(room)});
  socket.on('start',()=>{const room=rooms.get(socket.data.roomCode);if(!room||room.hostId!==socket.id)return;if(room.phase!=='waiting'&&room.phase!=='finished')return;if(room.players.length<2)return emitError(socket,'至少需要2名玩家');room.players.forEach(p=>p.lives=room.settings.lives);room.round=0;startRound(room)});
  socket.on('answer',(data={},ack=()=>{})=>{const room=rooms.get(socket.data.roomCode);if(!room||room.phase!=='playing'||room.turnId!==socket.id)return ack({ok:false,error:'还没轮到你'});if(Date.now()>=room.deadline){explode(room);return ack({ok:false,error:'河豚已经爆炸了！'})}const answer=clean(data.answer);if(!answer||norm(answer).length<1)return ack({ok:false,error:'请输入答案'});if(room.used.some(x=>norm(x.answer)===norm(answer)))return ack({ok:false,error:'这个答案已经有人说过了'});const p=room.players.find(p=>p.id===socket.id);room.used.push({answer,name:p?.name||'玩家'});nextTurn(room,socket.id);room.lastEvent={type:'answer',name:p?.name||'玩家',answer};broadcast(room);ack({ok:true})});
  socket.on('leave',()=>leave(socket));socket.on('disconnect',()=>leave(socket));
});
server.listen(PORT,()=>console.log(`河豚炸弹派对运行于 http://localhost:${PORT}`));
