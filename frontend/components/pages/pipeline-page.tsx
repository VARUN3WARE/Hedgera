
// "use client"

// import React, { useEffect, useState, useRef } from 'react'
// import {
//   ReactFlow,
//   useNodesState,
//   useEdgesState,
//   Background,
//   Controls,
//   MarkerType,
//   Node,
//   Handle,
//   Position,
//   NodeProps,
// } from '@xyflow/react'
// import '@xyflow/react/dist/style.css'
// import { 
//   Terminal, Activity, Cpu, Database, Globe, 
//   ShieldCheck, MessageSquare, Newspaper, Zap, 
//   TrendingUp, AlertCircle, CheckCircle2, Gavel, 
//   Scale, FileText, ArrowRightLeft, XCircle
// } from 'lucide-react'

// // --- 1. DATA TYPES ---
// type AnalystState = {
//   status: 'idle' | 'working' | 'done';
//   message: string;
// }

// type TickerData = {
//   news: AnalystState;
//   social: AnalystState;
//   market: AnalystState;
//   sec: AnalystState;
//   debate: string[];      // Chat history
//   debateSummary?: string; // High-level summary
//   validator: string;     // Final decision
//   status: 'pending' | 'analyzing' | 'debating' | 'validated';
// }

// type TradeDecision = {
//   ticker: string;
//   finrl: string;
//   validator: string;
//   final: 'APPROVED' | 'REJECTED';
// }

// // --- 2. CUSTOM COMPONENT: CONSENSUS ENGINE ---
// const ConsensusNode = ({ data }: NodeProps) => {
//   const [selectedTicker, setSelectedTicker] = useState<string | null>(null)
//   const tickers = data.tickers as Record<string, TickerData> || {}
//   const tickerList = Object.keys(tickers)
  
//   useEffect(() => {
//     if (!selectedTicker && tickerList.length > 0) {
//       setSelectedTicker(tickerList[0])
//     }
//   }, [tickerList, selectedTicker])

//   const activeData = selectedTicker ? tickers[selectedTicker] : null

//   return (
//     <div className="relative min-w-[850px] bg-slate-900/95 border border-slate-700 rounded-xl overflow-hidden shadow-2xl ring-1 ring-white/10 flex flex-col">
//       <Handle type="target" position={Position.Top} className="!bg-blue-500 !w-3 !h-3" />
      
//       {/* HEADER: Ticker Tabs */}
//       <div className="bg-slate-950 border-b border-slate-800 p-2 flex items-center gap-2 overflow-x-auto scrollbar-hide">
//         <div className="flex items-center gap-2 pr-4 border-r border-slate-800 mr-2 pl-2">
//            <div className="p-1.5 bg-indigo-500/20 rounded-md"><Globe size={14} className="text-indigo-400"/></div>
//            <span className="text-[10px] font-bold text-slate-400 tracking-wider">ACTIVE STOCKS</span>
//         </div>
        
//         {tickerList.length === 0 && (
//           <span className="text-[11px] text-slate-600 italic px-2 animate-pulse">Waiting for FinRL selection...</span>
//         )}
        
//         {tickerList.map(t => (
//           <button
//             key={t}
//             onClick={() => setSelectedTicker(t)}
//             className={`px-4 py-1.5 rounded-full text-[10px] font-bold transition-all border flex items-center gap-2 ${
//               selectedTicker === t 
//               ? 'bg-blue-600 text-white border-blue-500 shadow-md shadow-blue-900/40' 
//               : 'bg-slate-900 text-slate-400 border-slate-700 hover:border-slate-500 hover:text-slate-200'
//             }`}
//           >
//             {t}
//             <span className={`w-1.5 h-1.5 rounded-full ${
//                tickers[t].status === 'validated' ? 'bg-emerald-400' : 
//                tickers[t].status === 'debating' ? 'bg-amber-400 animate-pulse' : 'bg-slate-500'
//             }`} />
//           </button>
//         ))}
//       </div>

//       <div className="p-5 space-y-5 bg-slate-900/50">
        
//         {/* PHASE 1: ANALYST GRID */}
//         <div>
//             <div className="flex items-center gap-2 mb-3 text-xs font-bold text-slate-500 uppercase tracking-widest">
//                 <FileText size={12} /> Phase 1: Multi-Agent Analysis
//             </div>
//             <div className="grid grid-cols-4 gap-3">
//                 <AnalystCard title="News" icon={Newspaper} color="text-blue-400" state={activeData?.news} defaultMsg="Waiting..." />
//                 <AnalystCard title="Social" icon={MessageSquare} color="text-pink-400" state={activeData?.social} defaultMsg="Waiting..." />
//                 <AnalystCard title="Market" icon={TrendingUp} color="text-emerald-400" state={activeData?.market} defaultMsg="Waiting..." />
//                 <AnalystCard title="SEC" icon={ShieldCheck} color="text-amber-400" state={activeData?.sec} defaultMsg="Waiting..." />
//             </div>
//         </div>

//         <div className="h-px bg-slate-800 w-full" />

//         {/* PHASE 2 & 3: DEBATE & VERDICT */}
//         <div className="grid grid-cols-3 gap-4 h-[280px]">
            
//             {/* Debate Summary & Log */}
//             <div className="col-span-2 flex flex-col bg-black/20 border border-slate-800 rounded-lg overflow-hidden">
//                 <div className="p-3 border-b border-slate-800 flex items-center justify-between bg-slate-900/50">
//                     <div className="flex items-center gap-2 text-xs font-bold text-slate-400 uppercase tracking-widest">
//                         <Scale size={12} /> Phase 2: Bull vs Bear Debate
//                     </div>
//                     {activeData?.status === 'debating' && <span className="text-[9px] text-amber-400 animate-pulse">● LIVE</span>}
//                 </div>
                
//                 <div className="flex-1 overflow-y-auto p-3 scrollbar-thin scrollbar-thumb-slate-700">
//                     {/* Summary Section */}
//                     {activeData?.debateSummary && (
//                         <div className="mb-4 p-3 bg-slate-800/30 rounded border border-slate-700/50">
//                              <h4 className="text-[10px] font-bold text-indigo-300 mb-1">EXECUTIVE SUMMARY</h4>
//                              <p className="text-[10px] text-slate-300 leading-relaxed font-medium">
//                                  {activeData.debateSummary}
//                              </p>
//                         </div>
//                     )}

//                     {/* Chat Log Section */}
//                     <div className="space-y-2">
//                         {(!activeData?.debate || activeData.debate.length === 0) && !activeData?.debateSummary && (
//                             <span className="text-[10px] text-slate-600 italic">Waiting for agents to begin debate...</span>
//                         )}
//                         {activeData?.debate?.map((line, i) => (
//                             <div key={i} className={`p-2 rounded border-l-2 text-[10px] leading-relaxed ${
//                                 line.includes('BULL') ? 'border-emerald-500 bg-emerald-900/5 text-emerald-100' :
//                                 line.includes('BEAR') ? 'border-rose-500 bg-rose-900/5 text-rose-100' :
//                                 'border-slate-500 text-slate-300'
//                             }`}>
//                                 <span className={`font-bold mr-1 ${
//                                     line.includes('BULL') ? 'text-emerald-400' : 
//                                     line.includes('BEAR') ? 'text-rose-400' : 'text-slate-400'
//                                 }`}>
//                                     {line.includes('BULL') ? 'BULL AGENT' : line.includes('BEAR') ? 'BEAR AGENT' : 'INFO'}:
//                                 </span>
//                                 <span className="opacity-90">
//                                     {line.replace(/.*BULL:|.*BEAR:|.*INFO -/, '').trim()}
//                                 </span>
//                             </div>
//                         ))}
//                     </div>
//                 </div>
//             </div>

//             {/* Verdict Card */}
//             <div className="col-span-1 flex flex-col bg-indigo-950/10 border border-indigo-500/30 rounded-lg p-3 relative overflow-hidden">
//                 <div className="absolute -right-4 -top-4 w-20 h-20 bg-indigo-500/20 blur-3xl rounded-full" />
                
//                 <div className="flex items-center gap-2 text-xs font-bold text-indigo-300 uppercase tracking-widest mb-4 z-10">
//                     <Gavel size={12} /> Phase 3: Verdict
//                 </div>

//                 <div className="flex-1 flex flex-col items-center justify-center text-center z-10">
//                     {!activeData?.validator ? (
//                         <div className="flex flex-col items-center gap-2 opacity-50">
//                             <div className="w-8 h-8 rounded-full border-2 border-slate-700 border-t-indigo-500 animate-spin" />
//                             <span className="text-[10px] text-slate-500 font-mono">VALIDATING...</span>
//                         </div>
//                     ) : (
//                         <div className="animate-in zoom-in duration-300">
//                             <div className="text-[10px] text-slate-400 font-mono mb-1">FINAL DECISION</div>
//                             <div className={`text-2xl font-black tracking-tight mb-1 ${
//                                 activeData.validator.includes('BUY') || activeData.validator.includes('APPROVED') ? 'text-emerald-400 drop-shadow-[0_0_10px_rgba(52,211,153,0.5)]' :
//                                 activeData.validator.includes('SELL') ? 'text-rose-400 drop-shadow-[0_0_10px_rgba(251,113,133,0.5)]' :
//                                 'text-amber-400'
//                             }`}>
//                                 {activeData.validator.split('(')[0]}
//                             </div>
//                             <div className="inline-block px-2 py-0.5 rounded bg-white/5 border border-white/10 text-[9px] text-slate-300">
//                                 {activeData.validator.split('(')[1]?.replace(')', '') || 'Confirmed'}
//                             </div>
//                         </div>
//                     )}
//                 </div>
//             </div>

//         </div>
//       </div>
      
//       <Handle type="source" position={Position.Bottom} className="!bg-blue-500 !w-3 !h-3" />
//     </div>
//   )
// }

// // --- 3. CUSTOM COMPONENT: EXECUTION AGENT ---
// const ExecutionNode = ({ data }: NodeProps) => {
//   const trades = data.trades as TradeDecision[] || []
//   const approvedCount = trades.filter(t => t.final === 'APPROVED').length
  
//   return (
//     <div className="relative min-w-[320px] bg-slate-900 border-2 border-emerald-500/40 rounded-xl overflow-hidden shadow-2xl">
//       <Handle type="target" position={Position.Top} className="!bg-emerald-500" />
      
//       <div className="bg-emerald-950/40 p-3 border-b border-emerald-500/30 flex items-center justify-between">
//         <div className="flex items-center gap-2">
//           <Activity size={16} className="text-emerald-400" />
//           <span className="font-bold text-emerald-100 text-xs tracking-wide">EXECUTION AGENT</span>
//         </div>
//         <div className="flex gap-2">
//            <span className="text-[9px] bg-emerald-500/20 text-emerald-300 px-1.5 rounded border border-emerald-500/20">
//              {approvedCount} ORDERS
//            </span>
//         </div>
//       </div>
      
//       <div className="p-2 space-y-2 max-h-[300px] overflow-y-auto scrollbar-thin scrollbar-thumb-emerald-900/50">
//         {trades.length === 0 && (
//             <div className="p-8 text-center text-[10px] text-slate-500 italic">
//                Waiting for reconciliation...
//             </div>
//         )}
//         {trades.map((t, i) => (
//           <div key={i} className="flex flex-col bg-slate-950/50 rounded border border-white/5 p-2.5 gap-1.5 transition-all hover:bg-slate-900">
//              <div className="flex justify-between items-center">
//                 <span className="font-bold text-white text-sm">{t.ticker}</span>
//                 <span className={`px-2 py-0.5 rounded text-[9px] font-bold border ${
//                     t.final === 'APPROVED' 
//                     ? 'bg-emerald-500 text-black border-emerald-400' 
//                     : 'bg-red-500/10 text-red-400 border-red-500/20'
//                 }`}>
//                     {t.final}
//                 </span>
//              </div>
//              {/* Reconciliation Detail */}
//              <div className="flex justify-between text-[9px] text-slate-400 font-mono bg-black/20 p-1.5 rounded">
//                 <span className={t.finrl.includes('BUY') ? 'text-green-300' : ''}>🤖 FinRL: {t.finrl}</span>
//                 <span className={t.validator.includes('BUY') ? 'text-green-300' : t.validator.includes('HOLD') ? 'text-amber-300' : 'text-red-300'}>
//                     ⚖️ Val: {t.validator}
//                 </span>
//              </div>
//           </div>
//         ))}
//       </div>
//     </div>
//   )
// }

// // Helper: Analyst Card
// const AnalystCard = ({ title, icon: Icon, color, state, defaultMsg }: any) => {
//   const status = state?.status || 'idle'
//   const msg = state?.message || defaultMsg
  
//   const getBorderColor = () => {
//     if (status === 'working') return 'border-blue-500/50 bg-blue-500/5'
//     if (status === 'done') return 'border-green-500/30 bg-green-500/5'
//     return 'border-slate-800 bg-slate-900/50'
//   }

//   return (
//     <div className={`p-2.5 rounded border transition-all duration-500 ${getBorderColor()}`}>
//       <div className="flex items-center justify-between mb-1.5">
//         <div className="flex items-center gap-1.5">
//           <Icon size={12} className={color} />
//           <span className={`text-[10px] font-bold ${color} opacity-90`}>{title}</span>
//         </div>
//         {status === 'done' && <CheckCircle2 size={10} className="text-green-500" />}
//         {status === 'working' && <Activity size={10} className="text-blue-400 animate-spin" />}
//       </div>
//       <div className="text-[9px] text-slate-400 font-medium leading-tight pl-1 border-l border-slate-700/50 h-[2.2em] overflow-hidden">
//         {msg}
//       </div>
//     </div>
//   )
// }

// // --- 4. STANDARD NODE ---
// const StandardNode = ({ data }: NodeProps) => {
//   const getStatusStyles = (status: string) => {
//     switch (status) {
//       case 'active': return 'border-blue-500 shadow-[0_0_20px_rgba(59,130,246,0.3)] bg-blue-950/20'
//       case 'success': return 'border-emerald-500 shadow-[0_0_20px_rgba(16,185,129,0.3)] bg-emerald-950/20'
//       case 'loading': return 'border-amber-500 shadow-[0_0_15px_rgba(245,158,11,0.3)] bg-amber-950/20'
//       default: return 'border-slate-800 bg-slate-900/90'
//     }
//   }

//   const Icon = data.icon as any;

//   return (
//     <div className={`relative min-w-[200px] rounded-xl border-2 backdrop-blur-md transition-all duration-500 ${getStatusStyles(data.status as string)}`}>
//       <Handle type="target" position={Position.Top} className="!bg-slate-400 !w-2 !h-2" />
//       <div className="p-4">
//         <div className="flex items-center gap-3 mb-3">
//           <div className="p-2 rounded-lg bg-slate-950 border border-slate-800">
//             {Icon && <Icon size={18} className="text-slate-200" />}
//           </div>
//           <div>
//             <h3 className="text-xs font-bold text-slate-100">{data.label as string}</h3>
//             <p className="text-[10px] text-slate-500 font-bold uppercase tracking-wider">{data.subTitle as string}</p>
//           </div>
//         </div>
//         <div className="bg-black/40 rounded p-2 border border-white/5">
//            <div className="text-[10px] font-medium text-slate-300 min-h-[1.5em] leading-tight">
//              {data.liveMsg as string || <span className="text-slate-600 italic">Idle</span>}
//            </div>
//         </div>
//       </div>
//       <Handle type="source" position={Position.Bottom} className="!bg-slate-400 !w-2 !h-2" />
//     </div>
//   )
// }

// const nodeTypes = { 
//   consensus: ConsensusNode,
//   execution: ExecutionNode,
//   standard: StandardNode 
// }

// // --- 5. GRAPH LAYOUT ---
// const initialNodes: Node[] = [
//   // ROW 1
//   { id: 'producers', type: 'standard', position: { x: 0, y: 0 }, data: { label: 'Data Sources', subTitle: 'INGESTION', icon: Globe, status: 'idle', liveMsg: 'Connecting...' } },
//   { id: 'engine', type: 'standard', position: { x: 300, y: 0 }, data: { label: 'Stream Engine', subTitle: 'AGGREGATION', icon: Zap, status: 'idle', liveMsg: 'Waiting...' } },
  
//   // ROW 2
//   { id: 'mongodb', type: 'standard', position: { x: 150, y: 220 }, data: { label: 'MongoDB', subTitle: 'STORAGE', icon: Database, status: 'idle', liveMsg: 'Ready' } },
//   { id: 'finrl', type: 'standard', position: { x: 450, y: 220 }, data: { label: 'FinRL Model', subTitle: 'QUANT BRAIN', icon: Cpu, status: 'idle', liveMsg: 'Loading...' } },

//   // ROW 3 - The Consolidated Super-Node
//   { 
//     id: 'consensus', type: 'consensus', position: { x: 100, y: 450 }, 
//     data: { tickers: {} } 
//   },

//   // ROW 4 - Execution
//   { id: 'execution', type: 'execution', position: { x: 1050, y: 450 }, data: { trades: [] } },
// ]

// const initialEdges = [
//   { id: 'e1', source: 'producers', target: 'engine', animated: true, style: { stroke: '#3b82f6' } },
//   { id: 'e2', source: 'engine', target: 'mongodb', animated: true, style: { stroke: '#64748b', strokeDasharray: '5,5' } },
//   { id: 'e3', source: 'engine', target: 'finrl', animated: true, style: { stroke: '#3b82f6' } },
//   { id: 'e4', source: 'finrl', target: 'consensus', animated: true, style: { stroke: '#a855f7', strokeWidth: 2 } },
//   { id: 'e5', source: 'consensus', target: 'execution', animated: true, style: { stroke: '#22c55e', strokeWidth: 3 }, markerEnd: { type: MarkerType.ArrowClosed, color: '#22c55e' } },
// ]

// export default function PipelinePage() {
//   const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes)
//   const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges)
//   const [logs, setLogs] = useState<string[]>([])
//   const [activeTickers, setActiveTickers] = useState<Record<string, TickerData>>({})
//   const [tradeDecisions, setTradeDecisions] = useState<TradeDecision[]>([])
//   const [currentlyProcessingTicker, setCurrentlyProcessingTicker] = useState<string | null>(null)
//   const terminalEndRef = useRef<HTMLDivElement>(null)

//   useEffect(() => { terminalEndRef.current?.scrollIntoView({ behavior: 'smooth' }) }, [logs])

//   // --- WEBSOCKET ---
//   useEffect(() => {
//     const socket = new WebSocket('ws://localhost:8001/ws/logs')
//     socket.onopen = () => socket.send('START')
    
//     socket.onmessage = (event) => {
//       try {
//         const data = JSON.parse(event.data)

//         if(data.type==='file_update')
//         {
//             console.log('Received file update:', data.filename)
//             console.log('Data:', data.data)
//         }
        
//         // Handle Raw Logs (for animations & simple status)
//         if (data.type === 'log') {
//           const raw = data.raw
//           setLogs(p => [...p, raw].slice(-300))
//           parseLog(raw)
//           if (data.update && data.update.node !== 'analysts' && data.update.node !== 'debate') {
//              updateSimpleNode(data.update.node, data.update.status, data.update.msg)
//           }
//         }

//         // Handle JSON Files (Rich Data)
//         if (data.type === 'file_update') {
//             handleFileUpdate(data.filename, data.data)
//         }

//       } catch (e) {}
//     }
//     return () => socket.close()
//   }, [])

//   // --- JSON FILE HANDLER ---
//   const handleFileUpdate = (filename: string, jsonData: any) => {
//     // 1. DEBATE RESULTS (Summary, Validator, Chat)
//     if (filename === '07_debate_results.json') {
//         setActiveTickers(prev => {
//             const newState = { ...prev }
//             Object.entries(jsonData).forEach(([ticker, result]: [string, any]) => {
//                 if (!newState[ticker]) {
//                      const empty = { status: 'idle', message: 'Waiting...' } as AnalystState
//                      newState[ticker] = { news: empty, social: empty, market: empty, sec: empty, debate: [], validator: '', status: 'pending' }
//                 }
                
//                 // Set Summary
//                 if (result.summary) {
//                     newState[ticker].debateSummary = result.summary
//                     newState[ticker].status = 'debating'
//                 }
                
//                 // Set Chat History (Debate Log)
//                 if (result.debate_log && Array.isArray(result.debate_log)) {
//                     newState[ticker].debate = result.debate_log.map((d: any) => `${d.role}: ${d.content}`)
//                 }

//                 // Set Validator
//                 if (result.validation?.final_recommendation) {
//                     const rec = result.validation.final_recommendation
//                     newState[ticker].validator = `${rec.decision} (${rec.conviction})`
//                     newState[ticker].status = 'validated'
//                 }
//             })
//             return newState
//         })
//     }

//     // 2. RECONCILIATION (Execution Card)
//     if (filename === '08_reconciliation.json') {
//         const newTrades: TradeDecision[] = []
        
//         jsonData.approved_stocks.forEach((item: any) => {
//             newTrades.push({
//                 ticker: item.ticker,
//                 finrl: `${item.action} (${item.finrl_shares})`,
//                 validator: `BUY (${item.validator_confidence})`,
//                 final: 'APPROVED'
//             })
//         })
        
//         jsonData.rejected_stocks.forEach((item: any) => {
//             newTrades.push({
//                 ticker: item.ticker,
//                 finrl: `${item.finrl_action}`,
//                 validator: `${item.validator_action}`,
//                 final: 'REJECTED'
//             })
//         })
//         setTradeDecisions(newTrades)
//     }
//   }

//   // --- LOG PARSER ---
//   const parseLog = (log: string) => {
//     // 1. Standard Updates
//     if (log.includes('C=$')) updateSimpleNode('producers', 'active', `Received ${log.split(':')[0].replace(/.*📊/, '')}`)
//     if (log.includes('Aggregated')) updateSimpleNode('engine', 'active', `Processing Batch`)
//     if (log.includes('Synced')) updateSimpleNode('mongodb', 'success', log.match(/Synced (\d+)/)?.[0] || 'Synced')
//     if (log.includes('Waiting')) updateSimpleNode('finrl', 'loading', 'Accumulating Data...')
//     if (log.includes('Selected tickers:')) updateSimpleNode('finrl', 'success', 'Targets Selected')

//     // 2. Track Ticker Context
//     let contextTicker = currentlyProcessingTicker;
//     if (log.includes('Processing:')) { contextTicker = log.split('Processing:')[1].trim(); setCurrentlyProcessingTicker(contextTicker) }
//     else if (log.includes('Debate for:')) { contextTicker = log.split('Debate for:')[1].trim(); setCurrentlyProcessingTicker(contextTicker) }
    
//     const bracketMatch = log.match(/\[([A-Z]{1,5})\]/)
//     if (bracketMatch) contextTicker = bracketMatch[1]

//     if (contextTicker) {
//         const t = contextTicker
//         setActiveTickers(prev => {
//             const newState = { ...prev }
//             if (!newState[t]) {
//                 const emptyAnalyst = { status: 'idle', message: 'Waiting...' } as AnalystState
//                 newState[t] = { news: emptyAnalyst, social: emptyAnalyst, market: emptyAnalyst, sec: emptyAnalyst, debate: [], validator: '', status: 'pending' }
//             }
//             const tickerData = { ...newState[t] } 
            
//             // Live Status Updates
//             if (log.includes('News')) tickerData.news = { status: 'working', message: 'Scanning...' }
//             if (log.includes('Social')) tickerData.social = { status: 'working', message: 'Scanning...' }
//             if (log.includes('Market')) tickerData.market = { status: 'working', message: 'Analyzing...' }
//             if (log.includes('SEC')) tickerData.sec = { status: 'working', message: 'Reviewing...' }
            
//             // Completion updates
//             if (log.includes('complete') || log.includes('Completed')) {
//                 if(log.includes('News')) tickerData.news = { status: 'done', message: 'Sentiment Analysis Done' }
//                 if(log.includes('Social')) tickerData.social = { status: 'done', message: 'Hype Score Calculated' }
//                 if(log.includes('Market')) tickerData.market = { status: 'done', message: 'Technical Signals Ready' }
//                 if(log.includes('SEC')) tickerData.sec = { status: 'done', message: 'Compliance Verified' }
//             }
//             newState[t] = tickerData
//             return newState
//         })
//     }
//   }

//   // Sync state to Custom Nodes
//   useEffect(() => {
//     setNodes(nds => nds.map(node => {
//       if (node.id === 'consensus') return { ...node, data: { ...node.data, tickers: activeTickers } }
//       if (node.id === 'execution') return { ...node, data: { ...node.data, trades: tradeDecisions } }
//       return node
//     }))
//   }, [activeTickers, tradeDecisions, setNodes])

//   const updateSimpleNode = (nodeId: string, status: string, msg: string) => {
//     if (['producers', 'engine', 'mongodb', 'finrl'].includes(nodeId)) {
//       setNodes(nds => nds.map(n => {
//         if (n.id === nodeId) return { ...n, data: { ...n.data, status, liveMsg: msg } }
//         return n
//       }))
//     }
//   }

//   return (
//     <div className="flex flex-col h-[calc(100vh-120px)] gap-6">
//       <div className="flex justify-between items-center bg-slate-900/50 p-4 rounded-xl border border-slate-800 shadow-lg backdrop-blur-md">
//          <div className="flex items-center gap-3"><div className="w-3 h-3 rounded-full bg-emerald-500 animate-pulse" /><h2 className="text-white font-bold text-sm tracking-wide">LIVE OPERATIONS CENTER</h2></div>
//          <div className="flex items-center gap-2 px-3 py-1 bg-black/40 rounded border border-slate-800 text-slate-400 text-xs font-mono"><Activity size={12} /> ACTIVE</div>
//       </div>
//       <div className="flex-1 flex gap-6 overflow-hidden">
//         <div className="flex-1 bg-slate-950 border border-slate-800 rounded-xl relative overflow-hidden shadow-inner">
//           <ReactFlow nodes={nodes} edges={edges} nodeTypes={nodeTypes} fitView className="bg-slate-950"><Background color="#1e293b" gap={24} size={1} /><Controls className="bg-slate-800 border-slate-700 fill-white" /></ReactFlow>
//         </div>
//         <div className="w-[350px] bg-black border border-slate-800 rounded-xl flex flex-col shadow-xl">
//            <div className="p-3 bg-slate-900 border-b border-slate-800 text-xs font-bold text-slate-400 flex items-center gap-2"><Terminal size={14} /> SYSTEM EVENT LOG</div>
//            <div className="flex-1 overflow-y-auto p-3 font-mono text-[10px] space-y-1">
//               {logs.map((l, i) => (
//                 <div key={i} className={`break-words border-l-2 pl-2 py-0.5 ${l.includes('ERROR') ? 'border-red-500 text-red-400' : l.includes('SUCCESS') ? 'border-emerald-500 text-emerald-400' : l.includes('WARNING') ? 'border-amber-500 text-amber-400' : 'border-slate-800 text-slate-500'}`}>{l.replace('[PIPE]', '')}</div>
//               ))}
//               <div ref={terminalEndRef}/>
//            </div>
//         </div>
//       </div>
//     </div>
//   )
// }

// "use client"

// import React, { useEffect, useState, useRef } from 'react'
// import {
//   ReactFlow,
//   useNodesState,
//   useEdgesState,
//   Background,
//   Controls,
//   MarkerType,
//   Node,
//   Handle,
//   Position,
//   NodeProps,
// } from '@xyflow/react'
// import '@xyflow/react/dist/style.css'
// import { 
//   Terminal, Activity, Cpu, Database, Globe, 
//   ShieldCheck, MessageSquare, Newspaper, Zap, 
//   TrendingUp, AlertCircle, CheckCircle2, Gavel, 
//   Scale, FileText, XCircle
// } from 'lucide-react'

// // --- 1. DATA TYPES ---
// type AnalystState = {
//   status: 'idle' | 'working' | 'done';
//   message: string;
// }

// type TickerData = {
//   news: AnalystState;
//   social: AnalystState;
//   market: AnalystState;
//   sec: AnalystState;
//   debate: string[];      
//   debateSummary?: string; 
//   validator: string;     
//   status: 'pending' | 'analyzing' | 'debating' | 'validated';
// }

// type TradeDecision = {
//   ticker: string;
//   action: string;
//   reason: string;
//   status: 'APPROVED' | 'REJECTED' | 'EXECUTED' | 'FAILED';
//   executionMsg?: string;
//   finrl?: string;
//   validator?: string;
// }

// // --- 2. CUSTOM COMPONENT: CONSENSUS ENGINE ---
// const ConsensusNode = ({ data }: NodeProps) => {
//   const [selectedTicker, setSelectedTicker] = useState<string | null>(null)
//   const tickers = data.tickers as Record<string, TickerData> || {}
//   const tickerList = Object.keys(tickers)
  
//   useEffect(() => {
//     // Auto-select first ticker if none selected
//     if ((!selectedTicker || !tickers[selectedTicker]) && tickerList.length > 0) {
//       setSelectedTicker(tickerList[0])
//     }
//   }, [tickerList, selectedTicker, tickers])

//   const activeData = selectedTicker ? tickers[selectedTicker] : null

//   return (
//     <div className="relative min-w-[850px] bg-slate-900/95 border border-slate-700 rounded-xl overflow-hidden shadow-2xl ring-1 ring-white/10 flex flex-col">
//       <Handle type="target" position={Position.Top} className="!bg-blue-500 !w-3 !h-3" />
      
//       {/* HEADER: Ticker Tabs */}
//       <div className="bg-slate-950 border-b border-slate-800 p-2 flex items-center gap-2 overflow-x-auto scrollbar-hide">
//         <div className="flex items-center gap-2 pr-4 border-r border-slate-800 mr-2 pl-2 shrink-0">
//            <div className="p-1.5 bg-indigo-500/20 rounded-md"><Globe size={14} className="text-indigo-400"/></div>
//            <span className="text-[10px] font-bold text-slate-400 tracking-wider">ACTIVE STOCKS</span>
//         </div>
        
//         {tickerList.length === 0 && (
//           <span className="text-[11px] text-slate-600 italic px-2 animate-pulse">Waiting for FinRL selection...</span>
//         )}
        
//         {tickerList.map(t => (
//           <button
//             key={t}
//             onClick={() => setSelectedTicker(t)}
//             className={`px-4 py-1.5 rounded-full text-[10px] font-bold transition-all border flex items-center gap-2 ${
//               selectedTicker === t 
//               ? 'bg-blue-600 text-white border-blue-500 shadow-md shadow-blue-900/40' 
//               : 'bg-slate-900 text-slate-400 border-slate-700 hover:border-slate-500 hover:text-slate-200'
//             }`}
//           >
//             {t}
//             <span className={`w-1.5 h-1.5 rounded-full ${
//                tickers[t].status === 'validated' ? 'bg-emerald-400' : 
//                tickers[t].status === 'debating' ? 'bg-amber-400 animate-pulse' : 
//                tickers[t].status === 'analyzing' ? 'bg-blue-400 animate-pulse' : 'bg-slate-500'
//             }`} />
//           </button>
//         ))}
//       </div>

//       <div className="p-5 space-y-5 bg-slate-900/50">
        
//         {/* PHASE 1: ANALYSTS */}
//         <div>
//             <div className="flex items-center gap-2 mb-3 text-xs font-bold text-slate-500 uppercase tracking-widest">
//                 <FileText size={12} /> Phase 1: Multi-Agent Analysis
//             </div>
//             <div className="grid grid-cols-4 gap-3">
//                 <AnalystCard title="News" icon={Newspaper} color="text-blue-400" state={activeData?.news} defaultMsg="Waiting..." />
//                 <AnalystCard title="Social" icon={MessageSquare} color="text-pink-400" state={activeData?.social} defaultMsg="Waiting..." />
//                 <AnalystCard title="Market" icon={TrendingUp} color="text-emerald-400" state={activeData?.market} defaultMsg="Waiting..." />
//                 <AnalystCard title="SEC" icon={ShieldCheck} color="text-amber-400" state={activeData?.sec} defaultMsg="Waiting..." />
//             </div>
//         </div>

//         <div className="h-px bg-slate-800 w-full" />

//         {/* PHASE 2 & 3: DEBATE & VERDICT */}
//         <div className="grid grid-cols-3 gap-4 h-[280px]">
            
//             {/* Debate Section */}
//             <div className="col-span-2 flex flex-col bg-black/20 border border-slate-800 rounded-lg p-4 min-h-0 overflow-hidden">
//                 <div className="flex items-center justify-between mb-3 shrink-0">
//                     <div className="flex items-center gap-2 text-xs font-bold text-slate-400 uppercase tracking-widest">
//                         <Scale size={12} /> Phase 2: Bull vs Bear Debate
//                     </div>
//                     {activeData?.status === 'debating' && <span className="text-[9px] text-amber-400 animate-pulse">● LIVE</span>}
//                 </div>
                
//                 <div className="flex-1 overflow-y-auto pr-2 custom-scrollbar">
//                     {/* Summary */}
//                     {activeData?.debateSummary && (
//                         <div className="mb-4 p-3 bg-slate-800/30 rounded border border-slate-700/50">
//                              <h4 className="text-[10px] font-bold text-indigo-300 mb-1">EXECUTIVE SUMMARY</h4>
//                              <p className="text-[10px] text-slate-300 leading-relaxed font-mono whitespace-pre-line">
//                                  {activeData.debateSummary}
//                              </p>
//                         </div>
//                     )}
//                     {/* Chat */}
//                     <div className="space-y-2">
//                         {(!activeData?.debate || activeData.debate.length === 0) && !activeData?.debateSummary && (
//                             <span className="text-[10px] text-slate-600 italic">Waiting for agents to begin debate...</span>
//                         )}
//                         {activeData?.debate?.map((line, i) => (
//                             <div key={i} className={`p-2 rounded border-l-2 text-[10px] leading-relaxed ${
//                                 line.includes('BULL') ? 'border-emerald-500 bg-emerald-900/5 text-emerald-100' :
//                                 line.includes('BEAR') ? 'border-rose-500 bg-rose-900/5 text-rose-100' :
//                                 'border-slate-500 text-slate-300'
//                             }`}>
//                                 <span className={`font-bold mr-1 ${
//                                     line.includes('BULL') ? 'text-emerald-400' : 
//                                     line.includes('BEAR') ? 'text-rose-400' : 'text-slate-400'
//                                 }`}>
//                                     {line.includes('BULL') ? 'BULL AGENT' : line.includes('BEAR') ? 'BEAR AGENT' : 'INFO'}:
//                                 </span>
//                                 <span className="opacity-90">
//                                     {line.replace(/.*BULL:|.*BEAR:|.*INFO -/, '').trim()}
//                                 </span>
//                             </div>
//                         ))}
//                     </div>
//                 </div>
//             </div>

//             {/* Verdict Card */}
//             <div className="col-span-1 flex flex-col bg-indigo-950/10 border border-indigo-500/30 rounded-lg p-3 relative overflow-hidden">
//                 <div className="absolute -right-4 -top-4 w-20 h-20 bg-indigo-500/20 blur-3xl rounded-full" />
//                 <div className="flex items-center gap-2 text-xs font-bold text-indigo-300 uppercase tracking-widest mb-4 z-10">
//                     <Gavel size={12} /> Phase 3: Verdict
//                 </div>
//                 <div className="flex-1 flex flex-col items-center justify-center text-center z-10">
//                     {!activeData?.validator ? (
//                         <div className="flex flex-col items-center gap-2 opacity-50">
//                             <div className="w-8 h-8 rounded-full border-2 border-slate-700 border-t-indigo-500 animate-spin" />
//                             <span className="text-[10px] text-slate-500 font-mono">VALIDATING...</span>
//                         </div>
//                     ) : (
//                         <div className="animate-in zoom-in duration-300">
//                             <div className="text-[10px] text-slate-400 font-mono mb-1">FINAL DECISION</div>
//                             <div className={`text-2xl font-black tracking-tight mb-1 ${
//                                 activeData.validator.includes('BUY') || activeData.validator.includes('APPROVED') ? 'text-emerald-400 drop-shadow-[0_0_10px_rgba(52,211,153,0.5)]' :
//                                 activeData.validator.includes('SELL') ? 'text-rose-400 drop-shadow-[0_0_10px_rgba(251,113,133,0.5)]' :
//                                 'text-amber-400'
//                             }`}>
//                                 {activeData.validator.split('(')[0]}
//                             </div>
//                             <div className="inline-block px-2 py-0.5 rounded bg-white/5 border border-white/10 text-[9px] text-slate-300">
//                                 {activeData.validator.split('(')[1]?.replace(')', '') || 'Confirmed'}
//                             </div>
//                         </div>
//                     )}
//                 </div>
//             </div>
//         </div>
//       </div>
//       <Handle type="source" position={Position.Bottom} className="!bg-blue-500 !w-3 !h-3" />
//     </div>
//   )
// }

// // --- 3. CUSTOM COMPONENT: EXECUTION AGENT ---
// const ExecutionNode = ({ data }: NodeProps) => {
//   const trades = data.trades as TradeDecision[] || []
//   const approvedCount = trades.filter(t => t.status === 'APPROVED' || t.status === 'EXECUTED').length
  
//   return (
//     <div className="relative min-w-[350px] bg-slate-900 border-2 border-emerald-500/40 rounded-xl overflow-hidden shadow-2xl flex flex-col">
//       <Handle type="target" position={Position.Top} className="!bg-emerald-500" />
      
//       <div className="bg-emerald-950/40 p-3 border-b border-emerald-500/30 flex items-center justify-between shrink-0">
//         <div className="flex items-center gap-2">
//           <Activity size={16} className="text-emerald-400" />
//           <span className="font-bold text-emerald-100 text-xs tracking-wide">EXECUTION AGENT</span>
//         </div>
//         <div className="flex gap-2">
//            <span className="text-[9px] bg-emerald-500/20 text-emerald-300 px-1.5 rounded border border-emerald-500/20">
//              {approvedCount} ORDERS
//            </span>
//         </div>
//       </div>
      
//       <div className="p-2 space-y-2 max-h-[400px] overflow-y-auto custom-scrollbar">
//         {trades.length === 0 && (
//             <div className="p-8 text-center text-[10px] text-slate-500 italic">
//                Waiting for reconciliation...
//             </div>
//         )}
//         {trades.map((t, i) => (
//           <div key={i} className={`flex flex-col bg-slate-950/50 rounded border p-2.5 gap-1.5 transition-all hover:bg-slate-900 ${
//               t.status === 'FAILED' ? 'border-red-500/30' : 
//               t.status === 'EXECUTED' ? 'border-emerald-500/30' : 
//               'border-white/5'
//           }`}>
//              <div className="flex justify-between items-center">
//                 <span className="font-bold text-white text-sm">{t.ticker}</span>
//                 <span className={`px-2 py-0.5 rounded text-[9px] font-bold border ${
//                     t.status === 'EXECUTED' ? 'bg-emerald-500 text-black border-emerald-400' : 
//                     t.status === 'FAILED' ? 'bg-red-500 text-white border-red-400' :
//                     t.status === 'APPROVED' ? 'bg-blue-500/20 text-blue-300 border-blue-500/30' :
//                     'bg-slate-700 text-slate-400 border-slate-600'
//                 }`}>
//                     {t.status}
//                 </span>
//              </div>
             
//              {/* Error/Reason Msg */}
//              <div className="flex items-start gap-1.5 text-[10px] text-slate-400 leading-tight">
//                 {t.status === 'FAILED' ? <XCircle size={12} className="text-red-500 shrink-0 mt-0.5"/> : 
//                  t.status === 'EXECUTED' ? <CheckCircle2 size={12} className="text-emerald-500 shrink-0 mt-0.5"/> :
//                  <div className="w-3" />}
//                 <span className={t.status === 'FAILED' ? 'text-red-300' : ''}>
//                     {t.executionMsg || t.reason}
//                 </span>
//              </div>
             
//              {/* Decision Source */}
//              {t.action && (
//                  <div className="mt-1 pt-1 border-t border-white/5 flex justify-between text-[9px] font-mono text-slate-500">
//                     <span>TYPE: {t.action}</span>
//                  </div>
//              )}
//           </div>
//         ))}
//       </div>
//     </div>
//   )
// }

// // Helper: Analyst Card
// const AnalystCard = ({ title, icon: Icon, color, state, defaultMsg }: any) => {
//   const status = state?.status || 'idle'
//   const msg = state?.message || defaultMsg
//   const getBorderColor = () => { if (status === 'working') return 'border-blue-500/50 bg-blue-500/5'; if (status === 'done') return 'border-green-500/30 bg-green-500/5'; return 'border-slate-800 bg-slate-900/50' }
//   return (
//     <div className={`p-2.5 rounded border transition-all duration-500 ${getBorderColor()}`}>
//       <div className="flex items-center justify-between mb-1.5"><div className="flex items-center gap-1.5"><Icon size={12} className={color} /><span className={`text-[10px] font-bold ${color} opacity-90`}>{title}</span></div>{status === 'done' && <CheckCircle2 size={10} className="text-green-500" />}{status === 'working' && <Activity size={10} className="text-blue-400 animate-spin" />}</div>
//       <div className="text-[9px] text-slate-400 font-medium leading-tight pl-1 border-l border-slate-700/50 h-[2.2em] overflow-hidden">{msg}</div>
//     </div>
//   )
// }

// // --- 4. STANDARD NODE ---
// const StandardNode = ({ data }: NodeProps) => {
//   const getStatusStyles = (status: string) => { switch (status) { case 'active': return 'border-blue-500 shadow-blue-500/20'; case 'success': return 'border-emerald-500 shadow-emerald-500/20'; case 'loading': return 'border-amber-500 shadow-amber-500/20'; default: return 'border-slate-800 bg-slate-900/90' } }
//   const Icon = data.icon as any;
//   return (
//     <div className={`relative min-w-[200px] rounded-xl border-2 backdrop-blur-md transition-all duration-500 ${getStatusStyles(data.status as string)}`}>
//       <Handle type="target" position={Position.Top} className="!bg-slate-400 !w-2 !h-2" />
//       <div className="p-4"><div className="flex items-center gap-3 mb-3"><div className="p-2 rounded-lg bg-slate-950 border border-slate-800">{Icon && <Icon size={18} className="text-slate-200" />}</div><div><h3 className="text-xs font-bold text-slate-100">{data.label as string}</h3><p className="text-[10px] text-slate-500 font-bold uppercase tracking-wider">{data.subTitle as string}</p></div></div><div className="bg-black/40 rounded p-2 border border-white/5"><div className="text-[10px] font-medium text-slate-300 min-h-[1.5em] leading-tight">{data.liveMsg as string || <span className="text-slate-600 italic">Idle</span>}</div></div></div><Handle type="source" position={Position.Bottom} className="!bg-slate-400 !w-2 !h-2" />
//     </div>
//   )
// }

// const nodeTypes = { consensus: ConsensusNode, execution: ExecutionNode, standard: StandardNode }

// const initialNodes: Node[] = [
//   { id: 'producers', type: 'standard', position: { x: 0, y: 0 }, data: { label: 'Data Sources', subTitle: 'INGESTION', icon: Globe, status: 'idle', liveMsg: 'Connecting...' } },
//   { id: 'engine', type: 'standard', position: { x: 300, y: 0 }, data: { label: 'Stream Engine', subTitle: 'AGGREGATION', icon: Zap, status: 'idle', liveMsg: 'Waiting...' } },
//   { id: 'mongodb', type: 'standard', position: { x: 150, y: 220 }, data: { label: 'MongoDB', subTitle: 'STORAGE', icon: Database, status: 'idle', liveMsg: 'Ready' } },
//   { id: 'finrl', type: 'standard', position: { x: 450, y: 220 }, data: { label: 'FinRL Model', subTitle: 'QUANT BRAIN', icon: Cpu, status: 'idle', liveMsg: 'Loading...' } },
//   { id: 'consensus', type: 'consensus', position: { x: 100, y: 450 }, data: { tickers: {} } },
//   { id: 'execution', type: 'execution', position: { x: 1050, y: 450 }, data: { trades: [] } },
// ]

// const initialEdges = [
//   { id: 'e1', source: 'producers', target: 'engine', animated: true, style: { stroke: '#3b82f6' } },
//   { id: 'e2', source: 'engine', target: 'mongodb', animated: true, style: { stroke: '#64748b', strokeDasharray: '5,5' } },
//   { id: 'e3', source: 'engine', target: 'finrl', animated: true, style: { stroke: '#3b82f6' } },
//   { id: 'e4', source: 'finrl', target: 'consensus', animated: true, style: { stroke: '#a855f7', strokeWidth: 2 } },
//   { id: 'e5', source: 'consensus', target: 'execution', animated: true, style: { stroke: '#22c55e', strokeWidth: 3 }, markerEnd: { type: MarkerType.ArrowClosed, color: '#22c55e' } },
// ]

// export default function PipelinePage() {
//   const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes)
//   const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges)
//   const [logs, setLogs] = useState<string[]>([])
//   const [activeTickers, setActiveTickers] = useState<Record<string, TickerData>>({})
//   const [tradeDecisions, setTradeDecisions] = useState<TradeDecision[]>([])
//   const [currentlyProcessingTicker, setCurrentlyProcessingTicker] = useState<string | null>(null)
//   const terminalEndRef = useRef<HTMLDivElement>(null)

//   useEffect(() => { terminalEndRef.current?.scrollIntoView({ behavior: 'smooth' }) }, [logs])

//   // --- WEBSOCKET ---
//   useEffect(() => {
//     const socket = new WebSocket('ws://localhost:8001/ws/logs')
//     socket.onopen = () => socket.send('START')
    
//     socket.onmessage = (event) => {
//       try {
//         const data = JSON.parse(event.data)
        
//         // Handle Raw Logs
//         if (data.type === 'log') {
//           const raw = data.raw
//           setLogs(p => [...p, raw].slice(-300))
//           parseLog(raw)
//           if (data.update && data.update.node !== 'analysts' && data.update.node !== 'debate') {
//              updateSimpleNode(data.update.node, data.update.status, data.update.msg)
//           }
//         }

//         // HANDLE FILES (JSON)
//         if (data.type === 'file_update') {
//            const fname = data.filename
//            const json = data.data

//            // 1. Initial Ticker Setup (From FinRL)
//            if (fname === '01_finrl_output.json') {
//                const tickers = json.selected_tickers || []
//                setActiveTickers(prev => {
//                    const newState = { ...prev }
//                    tickers.forEach((t: string) => {
//                        if (!newState[t]) {
//                            const empty = { status: 'idle', message: 'Waiting...' } as AnalystState
//                            newState[t] = { news: empty, social: empty, market: empty, sec: empty, debate: [], validator: '', status: 'pending' }
//                        }
//                    })
//                    return newState
//                })
//            }

//            // 2. Agent Results (Debate/Validator)
//            if (fname.startsWith('05_agent_')) {
//               const ticker = json.ticker
//               if (ticker) updateTickerFromAgentFile(ticker, json)
//            }

//            // 3. Reconciliation (Initial State)
//            if (fname.includes('06_reconciliation')) {
//                const newTrades: TradeDecision[] = []
//                json.approved_stocks?.forEach((item: any) => {
//                    newTrades.push({ ticker: item.ticker, action: item.action, reason: item.reason, status: 'APPROVED' })
//                })
//                json.rejected_stocks?.forEach((item: any) => {
//                    newTrades.push({ ticker: item.ticker, action: item.finrl_action || 'HOLD', reason: item.reason, status: 'REJECTED' })
//                })
//                setTradeDecisions(newTrades)
//            }

//            // 4. Trade Execution (Final State)
//            if (fname.includes('07_trades')) {
//                const results = json.trade_results || []
//                setTradeDecisions(prev => {
//                    // Fix for TS error: explicit 'any' cast
//                    const tradeResultMap = new Map<string, any>(results.map((r: any) => [r.ticker, r]))
                   
//                    return prev.map(t => {
//                        const result = tradeResultMap.get(t.ticker)
//                        if (result) {
//                            return {
//                                ...t,
//                                status: result.status === 'failed' ? 'FAILED' : 'EXECUTED',
//                                executionMsg: result.message
//                            }
//                        }
//                        return t
//                    })
//                })
//            }
//         }
//       } catch (e) {}
//     }
//     return () => socket.close()
//   }, [])

//   // --- HELPER: Update Ticker Data ---
//   const updateTickerFromAgentFile = (ticker: string, data: any) => {
//       setActiveTickers(prev => {
//           const newState = { ...prev }
//           if (!newState[ticker]) {
//                const empty = { status: 'idle', message: 'Waiting...' } as AnalystState
//                newState[ticker] = { news: empty, social: empty, market: empty, sec: empty, debate: [], validator: '', status: 'pending' }
//           }
          
//           const tData = { ...newState[ticker] }
//           if (data.news_analysis) tData.news = { status: 'done', message: 'Sentiment Analysis Complete' }
//           if (data.social_analysis) tData.social = { status: 'done', message: 'Hype Score Calculated' }
//           if (data.market_analysis) tData.market = { status: 'done', message: 'Tech Analysis Complete' }
//           if (data.sec_analysis) tData.sec = { status: 'done', message: '10-K Parsed' }

//           if (data.debate_result) {
//               const result = data.debate_result
              
//               // Correct path: result.validation.summary based on your JSON structure
//               if (result.validation?.summary) tData.debateSummary = result.validation.summary
//               else if (result.summary) tData.debateSummary = result.summary
              
//               if (result.debate_log && Array.isArray(result.debate_log)) {
//                   tData.debate = result.debate_log.map((d: any) => `${d.role}: ${d.content}`)
//                   tData.status = 'debating'
//               }

//               const val = result.validation?.final_recommendation
//               if (val) {
//                   tData.validator = `${val.decision} (${val.conviction})`
//                   tData.status = 'validated'
//               }
//           }
//           newState[ticker] = tData
//           return newState
//       })
//   }

//   const parseLog = (log: string) => {
//     if (log.includes('C=$')) updateSimpleNode('producers', 'active', `Received ${log.split(':')[0].replace(/.*📊/, '')}`)
//     if (log.includes('Aggregated')) updateSimpleNode('engine', 'active', `Processing Batch`)
//     if (log.includes('Synced')) updateSimpleNode('mongodb', 'success', 'Synced')
//     if (log.includes('Waiting')) updateSimpleNode('finrl', 'loading', 'Accumulating Data...')
//     if (log.includes('FinRL:')) {
//         const count = log.match(/(\d+) tickers/)?.[1] || '0'
//         updateSimpleNode('finrl', 'success', `Selected ${count} Tickers`)
//     }
//   }

//   const updateSimpleNode = (nodeId: string, status: string, msg: string) => {
//     if (['producers', 'engine', 'mongodb', 'finrl', 'execution'].includes(nodeId)) {
//       setNodes(nds => nds.map(n => {
//         if (n.id === nodeId) return { ...n, data: { ...n.data, status, liveMsg: msg } }
//         return n
//       }))
//     }
//   }

//   // Sync state to Custom Nodes
//   useEffect(() => {
//     setNodes(nds => nds.map(node => {
//       if (node.id === 'consensus') return { ...node, data: { ...node.data, tickers: activeTickers } }
//       if (node.id === 'execution') return { ...node, data: { ...node.data, trades: tradeDecisions } }
//       return node
//     }))
//   }, [activeTickers, tradeDecisions, setNodes])

//   return (
//     <div className="flex flex-col h-screen max-h-screen bg-slate-950 text-white overflow-hidden">
//        {/* HEADER */}
//        <div className="h-14 flex justify-between items-center px-6 border-b border-slate-800 bg-slate-900 shrink-0 z-50">
//            <div className="flex items-center gap-3">
//                <div className="w-2.5 h-2.5 rounded-full bg-emerald-500 animate-pulse shadow-[0_0_10px_#10b981]" />
//                <div>
//                   <h1 className="text-lg font-bold tracking-tight text-white">AEGIS OPERATIONS CENTER</h1>
//                   <p className="text-[10px] text-slate-400 font-mono">AUTONOMOUS TRADING PIPELINE</p>
//                </div>
//            </div>
//            <div className="px-3 py-1 bg-black/40 rounded border border-slate-800 text-xs font-mono text-slate-400 flex items-center gap-2">
//               <Activity size={14} className="text-emerald-500" /> SYSTEM ACTIVE
//            </div>
//        </div>

//        {/* MAIN AREA */}
//        <div className="flex-1 flex min-h-0">
//           {/* GRAPH AREA */}
//           <div className="flex-1 relative bg-slate-950 flex flex-col min-w-0">
//              <ReactFlow 
//                 nodes={nodes} 
//                 edges={edges} 
//                 nodeTypes={nodeTypes} 
//                 fitView 
//                 minZoom={0.5}
//                 className="bg-slate-950 w-full h-full"
//              >
//                 <Background color="#334155" gap={32} size={1} />
//                 <Controls className="bg-slate-800 border-slate-700 fill-white" />
//              </ReactFlow>
//           </div>

//           {/* SIDEBAR */}
//           <div className="w-[380px] bg-black border-l border-slate-800 flex flex-col shrink-0 z-10 shadow-xl">
//               <div className="p-3 bg-slate-900/80 backdrop-blur border-b border-slate-800 flex items-center gap-2 text-xs font-bold text-slate-300">
//                   <Terminal size={14} /> LIVE SYSTEM LOGS
//               </div>
//               <div className="flex-1 overflow-y-auto p-3 font-mono text-[10px] space-y-1">
//                   {logs.map((l, i) => (
//                      <div key={i} className={`break-words border-l-2 pl-2 py-0.5 leading-tight ${l.includes('ERROR') ? 'border-red-500 text-red-400 bg-red-900/10' : l.includes('SUCCESS') ? 'border-emerald-500 text-emerald-400' : l.includes('WARNING') ? 'border-amber-500 text-amber-400' : 'border-slate-700 text-slate-500'}`}>
//                          {l.replace('[PIPE]', '')}
//                      </div>
//                   ))}
//                   <div ref={terminalEndRef} />
//               </div>
//           </div>
//        </div>
//     </div>
//   )
// }

"use client"

import React, { useEffect, useState, useRef } from 'react'
import {
  ReactFlow,
  useNodesState,
  useEdgesState,
  Background,
  Controls,
  MarkerType,
  Node,
  Handle,
  Position,
  NodeProps,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { 
  Terminal, Activity, Cpu, Database, Globe, 
  ShieldCheck, MessageSquare, Newspaper, Zap, 
  TrendingUp, AlertCircle, CheckCircle2, Gavel, 
  Scale, FileText, XCircle
} from 'lucide-react'

// --- 1. DATA TYPES ---
type AnalystState = {
  status: 'idle' | 'working' | 'done';
  message: string;
}

type TickerData = {
  news: AnalystState;
  social: AnalystState;
  market: AnalystState;
  sec: AnalystState;
  debate: string[];      
  debateSummary?: string; 
  validator: string;     
  status: 'pending' | 'analyzing' | 'debating' | 'validated';
}

type TradeDecision = {
  ticker: string;
  action: string;
  reason: string;
  status: 'APPROVED' | 'REJECTED'; // Simplified status
  finrl_detail: string;
  validator_detail: string;
}

// --- 2. CUSTOM COMPONENT: CONSENSUS ENGINE ---
const ConsensusNode = ({ data }: NodeProps) => {
  const [selectedTicker, setSelectedTicker] = useState<string | null>(null)
  const tickers = data.tickers as Record<string, TickerData> || {}
  const tickerList = Object.keys(tickers)
  
  useEffect(() => {
    if ((!selectedTicker || !tickers[selectedTicker]) && tickerList.length > 0) {
      setSelectedTicker(tickerList[0])
    }
  }, [tickerList, selectedTicker, tickers])

  const activeData = selectedTicker ? tickers[selectedTicker] : null

  return (
    <div className="relative min-w-[850px] bg-slate-900/95 border border-slate-700 rounded-xl overflow-hidden shadow-2xl ring-1 ring-white/10 flex flex-col">
      <Handle type="target" position={Position.Top} className="!bg-blue-500 !w-3 !h-3" />
      
      {/* HEADER: Ticker Tabs */}
      <div className="bg-slate-950 border-b border-slate-800 p-2 flex items-center gap-2 overflow-x-auto scrollbar-hide">
        <div className="flex items-center gap-2 pr-4 border-r border-slate-800 mr-2 pl-2 shrink-0">
           <div className="p-1.5 bg-indigo-500/20 rounded-md"><Globe size={14} className="text-indigo-400"/></div>
           <span className="text-[10px] font-bold text-slate-400 tracking-wider">ACTIVE STOCKS</span>
        </div>
        
        {tickerList.length === 0 && (
          <span className="text-[11px] text-slate-600 italic px-2 animate-pulse">Waiting for FinRL selection...</span>
        )}
        
        {tickerList.map(t => (
          <button
            key={t}
            onClick={() => setSelectedTicker(t)}
            className={`px-4 py-1.5 rounded-full text-[10px] font-bold transition-all border flex items-center gap-2 ${
              selectedTicker === t 
              ? 'bg-blue-600 text-white border-blue-500 shadow-md shadow-blue-900/40' 
              : 'bg-slate-900 text-slate-400 border-slate-700 hover:border-slate-500 hover:text-slate-200'
            }`}
          >
            {t}
            <span className={`w-1.5 h-1.5 rounded-full ${
               tickers[t].status === 'validated' ? 'bg-emerald-400' : 
               tickers[t].status === 'debating' ? 'bg-amber-400 animate-pulse' : 
               tickers[t].status === 'analyzing' ? 'bg-blue-400 animate-pulse' : 'bg-slate-500'
            }`} />
          </button>
        ))}
      </div>

      <div className="p-5 space-y-5 bg-slate-900/50">
        {/* PHASE 1: ANALYSTS */}
        <div>
            <div className="flex items-center gap-2 mb-3 text-xs font-bold text-slate-500 uppercase tracking-widest">
                <FileText size={12} /> Phase 1: Multi-Agent Analysis
            </div>
            <div className="grid grid-cols-4 gap-3">
                <AnalystCard title="News" icon={Newspaper} color="text-blue-400" state={activeData?.news} defaultMsg="Waiting..." />
                <AnalystCard title="Social" icon={MessageSquare} color="text-pink-400" state={activeData?.social} defaultMsg="Waiting..." />
                <AnalystCard title="Market" icon={TrendingUp} color="text-emerald-400" state={activeData?.market} defaultMsg="Waiting..." />
                <AnalystCard title="SEC" icon={ShieldCheck} color="text-amber-400" state={activeData?.sec} defaultMsg="Waiting..." />
            </div>
        </div>

        <div className="h-px bg-slate-800 w-full" />

        {/* PHASE 2 & 3: DEBATE & VERDICT */}
        <div className="grid grid-cols-3 gap-4 h-[280px]">
            
            {/* Debate Section */}
            <div className="col-span-2 flex flex-col bg-black/20 border border-slate-800 rounded-lg p-4 min-h-0 overflow-hidden">
                <div className="flex items-center justify-between mb-3 shrink-0">
                    <div className="flex items-center gap-2 text-xs font-bold text-slate-400 uppercase tracking-widest">
                        <Scale size={12} /> Phase 2: Bull vs Bear Debate
                    </div>
                    {activeData?.status === 'debating' && <span className="text-[9px] text-amber-400 animate-pulse">● LIVE</span>}
                </div>
                
                <div className="flex-1 overflow-y-auto pr-2 custom-scrollbar">
                    {/* Summary */}
                    {activeData?.debateSummary && (
                        <div className="mb-4 p-3 bg-slate-800/30 rounded border border-slate-700/50">
                             <h4 className="text-[10px] font-bold text-indigo-300 mb-1">EXECUTIVE SUMMARY</h4>
                             <p className="text-[10px] text-slate-300 leading-relaxed font-mono whitespace-pre-line">
                                 {activeData.debateSummary}
                             </p>
                        </div>
                    )}
                    {/* Chat */}
                    <div className="space-y-2">
                        {(!activeData?.debate || activeData.debate.length === 0) && !activeData?.debateSummary && (
                            <span className="text-[10px] text-slate-600 italic">Waiting for agents to begin debate...</span>
                        )}
                        {activeData?.debate?.map((line, i) => (
                            <div key={i} className={`p-2 rounded border-l-2 text-[10px] leading-relaxed ${
                                line.includes('BULL') ? 'border-emerald-500 bg-emerald-900/5 text-emerald-100' :
                                line.includes('BEAR') ? 'border-rose-500 bg-rose-900/5 text-rose-100' :
                                'border-slate-500 text-slate-300'
                            }`}>
                                <span className={`font-bold mr-1 ${
                                    line.includes('BULL') ? 'text-emerald-400' : 
                                    line.includes('BEAR') ? 'text-rose-400' : 'text-slate-400'
                                }`}>
                                    {line.includes('BULL') ? 'BULL AGENT' : line.includes('BEAR') ? 'BEAR AGENT' : 'INFO'}:
                                </span>
                                <span className="opacity-90">
                                    {line.replace(/.*BULL:|.*BEAR:|.*INFO -/, '').trim()}
                                </span>
                            </div>
                        ))}
                    </div>
                </div>
            </div>

            {/* Verdict Card */}
            <div className="col-span-1 flex flex-col bg-indigo-950/10 border border-indigo-500/30 rounded-lg p-3 relative overflow-hidden">
                <div className="absolute -right-4 -top-4 w-20 h-20 bg-indigo-500/20 blur-3xl rounded-full" />
                <div className="flex items-center gap-2 text-xs font-bold text-indigo-300 uppercase tracking-widest mb-4 z-10">
                    <Gavel size={12} /> Phase 3: Validator Agent
                </div>
                <div className="flex-1 flex flex-col items-center justify-center text-center z-10">
                    {!activeData?.validator ? (
                        <div className="flex flex-col items-center gap-2 opacity-50">
                            <div className="w-8 h-8 rounded-full border-2 border-slate-700 border-t-indigo-500 animate-spin" />
                            <span className="text-[10px] text-slate-500 font-mono">VALIDATING...</span>
                        </div>
                    ) : (
                        <div className="animate-in zoom-in duration-300">
                            <div className="text-[10px] text-slate-400 font-mono mb-1">FINAL DECISION</div>
                            <div className={`text-2xl font-black tracking-tight mb-1 ${
                                activeData.validator.includes('BUY') || activeData.validator.includes('APPROVED') ? 'text-emerald-400 drop-shadow-[0_0_10px_rgba(52,211,153,0.5)]' :
                                activeData.validator.includes('SELL') ? 'text-rose-400 drop-shadow-[0_0_10px_rgba(251,113,133,0.5)]' :
                                'text-amber-400'
                            }`}>
                                {activeData.validator.split('(')[0]}
                            </div>
                            <div className="inline-block px-2 py-0.5 rounded bg-white/5 border border-white/10 text-[9px] text-slate-300">
                                {activeData.validator.split('(')[1]?.replace(')', '') || 'Confirmed'}
                            </div>
                        </div>
                    )}
                </div>
            </div>
        </div>
      </div>
      <Handle type="source" position={Position.Bottom} className="!bg-blue-500 !w-3 !h-3" />
    </div>
  )
}

// --- 3. CUSTOM COMPONENT: EXECUTION AGENT ---
const ExecutionNode = ({ data }: NodeProps) => {
  const trades = data.trades as TradeDecision[] || []
  const approvedCount = trades.filter(t => t.status === 'APPROVED').length
  const rejectedCount = trades.filter(t => t.status === 'REJECTED').length
  
  return (
    <div className="relative min-w-[350px] bg-slate-900 border-2 border-emerald-500/40 rounded-xl overflow-hidden shadow-2xl flex flex-col">
      <Handle type="target" position={Position.Top} className="!bg-emerald-500" />
      
      <div className="bg-emerald-950/40 p-3 border-b border-emerald-500/30 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-2">
          <Activity size={16} className="text-emerald-400" />
          <span className="font-bold text-emerald-100 text-xs tracking-wide">DECISION AGENT</span>
        </div>
        <div className="flex gap-2">
           <span className="text-[9px] bg-emerald-500/20 text-emerald-300 px-1.5 rounded border border-emerald-500/20">
             {approvedCount} APPROVED
           </span>
           <span className="text-[9px] bg-red-500/20 text-red-300 px-1.5 rounded border border-red-500/20">
             {rejectedCount} REJECTED
           </span>
        </div>
      </div>
      
      <div className="p-2 space-y-2 max-h-[400px] overflow-y-auto custom-scrollbar">
        {trades.length === 0 && (
            <div className="p-8 text-center text-[10px] text-slate-500 italic">
               Waiting for reconciliation...
            </div>
        )}
        {trades.map((t, i) => (
          <div key={i} className={`flex flex-col bg-slate-950/50 rounded border p-2.5 gap-1.5 transition-all hover:bg-slate-900 ${
              t.status === 'REJECTED' ? 'border-red-500/30' : 
              'border-emerald-500/30'
          }`}>
             <div className="flex justify-between items-center">
                <span className="font-bold text-white text-sm">{t.ticker}</span>
                <span className={`px-2 py-0.5 rounded text-[9px] font-bold border ${
                    t.status === 'APPROVED' 
                    ? 'bg-emerald-500 text-black border-emerald-400' 
                    : 'bg-red-500/10 text-red-400 border-red-500/20'
                }`}>
                    {t.status}
                </span>
             </div>
             
             {/* Detailed Reasoning: FinRL vs Validator */}
             <div className="grid grid-cols-2 gap-2 mt-1">
                 <div className="bg-black/40 rounded p-1.5 border border-white/5">
                    <div className="text-[8px] text-slate-500 font-bold mb-0.5">FINRL MODEL</div>
                    <div className={`text-[9px] font-mono ${t.finrl_detail.includes('BUY') ? 'text-green-400' : 'text-slate-300'}`}>
                        {t.finrl_detail}
                    </div>
                 </div>
                 <div className="bg-black/40 rounded p-1.5 border border-white/5">
                    <div className="text-[8px] text-slate-500 font-bold mb-0.5">VALIDATOR</div>
                    <div className={`text-[9px] font-mono ${t.validator_detail.includes('High') ? 'text-green-400' : t.validator_detail.includes('HOLD') ? 'text-amber-400' : 'text-slate-300'}`}>
                        {t.validator_detail}
                    </div>
                 </div>
             </div>

             {/* Final Reason */}
             <div className="flex items-start gap-1.5 text-[10px] text-slate-400 leading-tight pt-1 border-t border-white/5 mt-1">
                {t.status === 'REJECTED' ? <XCircle size={12} className="text-red-500 shrink-0 mt-0.5"/> : 
                 <CheckCircle2 size={12} className="text-emerald-500 shrink-0 mt-0.5"/>}
                <span className={t.status === 'REJECTED' ? 'text-red-300' : 'text-emerald-300'}>
                    {t.reason}
                </span>
             </div>
          </div>
        ))}
      </div>
    </div>
  )
}

// Helper: Analyst Card
const AnalystCard = ({ title, icon: Icon, color, state, defaultMsg }: any) => {
  const status = state?.status || 'idle'
  const msg = state?.message || defaultMsg
  const getBorderColor = () => { if (status === 'working') return 'border-blue-500/50 bg-blue-500/5'; if (status === 'done') return 'border-green-500/30 bg-green-500/5'; return 'border-slate-800 bg-slate-900/50' }
  return (
    <div className={`p-2.5 rounded border transition-all duration-500 ${getBorderColor()}`}>
      <div className="flex items-center justify-between mb-1.5"><div className="flex items-center gap-1.5"><Icon size={12} className={color} /><span className={`text-[10px] font-bold ${color} opacity-90`}>{title}</span></div>{status === 'done' && <CheckCircle2 size={10} className="text-green-500" />}{status === 'working' && <Activity size={10} className="text-blue-400 animate-spin" />}</div>
      <div className="text-[9px] text-slate-400 font-medium leading-tight pl-1 border-l border-slate-700/50 h-[2.2em] overflow-hidden">{msg}</div>
    </div>
  )
}

// --- 4. STANDARD NODE ---
const StandardNode = ({ data }: NodeProps) => {
  const getStatusStyles = (status: string) => { switch (status) { case 'active': return 'border-blue-500 shadow-blue-500/20'; case 'success': return 'border-emerald-500 shadow-emerald-500/20'; case 'loading': return 'border-amber-500 shadow-amber-500/20'; default: return 'border-slate-800 bg-slate-900/90' } }
  const Icon = data.icon as any;
  return (
    <div className={`relative min-w-[200px] rounded-xl border-2 backdrop-blur-md transition-all duration-500 ${getStatusStyles(data.status as string)}`}>
      <Handle type="target" position={Position.Top} className="!bg-slate-400 !w-2 !h-2" />
      <div className="p-4"><div className="flex items-center gap-3 mb-3"><div className="p-2 rounded-lg bg-slate-950 border border-slate-800">{Icon && <Icon size={18} className="text-slate-200" />}</div><div><h3 className="text-xs font-bold text-slate-100">{data.label as string}</h3><p className="text-[10px] text-slate-500 font-bold uppercase tracking-wider">{data.subTitle as string}</p></div></div><div className="bg-black/40 rounded p-2 border border-white/5"><div className="text-[10px] font-medium text-slate-300 min-h-[1.5em] leading-tight">{data.liveMsg as string || <span className="text-slate-600 italic">Idle</span>}</div></div></div><Handle type="source" position={Position.Bottom} className="!bg-slate-400 !w-2 !h-2" />
    </div>
  )
}

const nodeTypes = { consensus: ConsensusNode, execution: ExecutionNode, standard: StandardNode }

const initialNodes: Node[] = [
  { id: 'producers', type: 'standard', position: { x: 0, y: 0 }, data: { label: 'Data Sources', subTitle: 'INGESTION', icon: Globe, status: 'idle', liveMsg: 'Connecting...' } },
  { id: 'engine', type: 'standard', position: { x: 300, y: 0 }, data: { label: 'Stream Engine', subTitle: 'AGGREGATION', icon: Zap, status: 'idle', liveMsg: 'Waiting...' } },
  { id: 'mongodb', type: 'standard', position: { x: 150, y: 220 }, data: { label: 'MongoDB', subTitle: 'STORAGE', icon: Database, status: 'idle', liveMsg: 'Ready' } },
  { id: 'finrl', type: 'standard', position: { x: 450, y: 220 }, data: { label: 'FinRL Model', subTitle: 'QUANT BRAIN', icon: Cpu, status: 'idle', liveMsg: 'Loading...' } },
  { id: 'consensus', type: 'consensus', position: { x: 100, y: 450 }, data: { tickers: {} } },
  { id: 'execution', type: 'execution', position: { x: 1050, y: 450 }, data: { trades: [] } },
]

const initialEdges = [
  { id: 'e1', source: 'producers', target: 'engine', animated: true, style: { stroke: '#3b82f6' } },
  { id: 'e2', source: 'engine', target: 'mongodb', animated: true, style: { stroke: '#64748b', strokeDasharray: '5,5' } },
  { id: 'e3', source: 'engine', target: 'finrl', animated: true, style: { stroke: '#3b82f6' } },
  { id: 'e4', source: 'finrl', target: 'consensus', animated: true, style: { stroke: '#a855f7', strokeWidth: 2 } },
  { id: 'e5', source: 'consensus', target: 'execution', animated: true, style: { stroke: '#22c55e', strokeWidth: 3 }, markerEnd: { type: MarkerType.ArrowClosed, color: '#22c55e' } },
]

export default function PipelinePage() {
  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes)
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges)
  const [logs, setLogs] = useState<string[]>([])
  const [activeTickers, setActiveTickers] = useState<Record<string, TickerData>>({})
  const [tradeDecisions, setTradeDecisions] = useState<TradeDecision[]>([])
  const terminalEndRef = useRef<HTMLDivElement>(null)

  useEffect(() => { terminalEndRef.current?.scrollIntoView({ behavior: 'smooth' }) }, [logs])

  // --- WEBSOCKET ---
  useEffect(() => {
    const socket = new WebSocket('ws://localhost:8001/ws/logs')
    socket.onopen = () => socket.send('START')
    
    socket.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data)
        
        // Handle Raw Logs
        if (data.type === 'log') {
          const raw = data.raw
          setLogs(p => [...p, raw].slice(-300))
          parseLog(raw)
          if (data.update && data.update.node !== 'analysts' && data.update.node !== 'debate') {
             updateSimpleNode(data.update.node, data.update.status, data.update.msg)
          }
        }

        // HANDLE FILES (JSON)
        if (data.type === 'file_update') {
           const fname = data.filename
           const json = data.data

           // 1. Agent Results (Debate/Validator)
           if (fname.startsWith('05_agent_')) {
              const ticker = json.ticker
              if (ticker) updateTickerFromAgentFile(ticker, json)
           }

           // 2. Reconciliation (Updated with FinRL vs Validator Details)
           if (fname.includes('06_reconciliation')) {
               const newTrades: TradeDecision[] = []
               
               json.approved_stocks?.forEach((item: any) => {
                   newTrades.push({ 
                       ticker: item.ticker, 
                       action: item.action, 
                       reason: item.reason, 
                       status: 'APPROVED',
                       finrl_detail: `${item.action} (${item.finrl_shares} shares)`,
                       validator_detail: `${item.validator_confidence} Confidence`
                   })
               })
               
               json.rejected_stocks?.forEach((item: any) => {
                   newTrades.push({ 
                       ticker: item.ticker, 
                       action: item.finrl_action || 'HOLD', 
                       reason: item.reason, 
                       status: 'REJECTED',
                       finrl_detail: item.finrl_action || 'HOLD',
                       validator_detail: item.validator_action || 'HOLD'
                   })
               })
               setTradeDecisions(newTrades)
           }
           
           // IGNORE 07_trades.json entirely as requested
        }
      } catch (e) {}
    }
    return () => socket.close()
  }, [])

  // --- HELPER: Update Ticker Data ---
  const updateTickerFromAgentFile = (ticker: string, data: any) => {
      setActiveTickers(prev => {
          const newState = { ...prev }
          if (!newState[ticker]) {
               const empty = { status: 'idle', message: 'Waiting...' } as AnalystState
               newState[ticker] = { news: empty, social: empty, market: empty, sec: empty, debate: [], validator: '', status: 'pending' }
          }
          
          const tData = { ...newState[ticker] }
          if (data.news_analysis) tData.news = { status: 'done', message: 'Sentiment Analysis Complete' }
          if (data.social_analysis) tData.social = { status: 'done', message: 'Hype Score Calculated' }
          if (data.market_analysis) tData.market = { status: 'done', message: 'Tech Analysis Complete' }
          if (data.sec_analysis) tData.sec = { status: 'done', message: '10-K Parsed' }

          if (data.debate_result) {
              const result = data.debate_result
              
              if (result.validation?.summary) tData.debateSummary = result.validation.summary
              else if (result.summary) tData.debateSummary = result.summary
              
              if (result.debate_log && Array.isArray(result.debate_log)) {
                  tData.debate = result.debate_log.map((d: any) => `${d.role}: ${d.content}`)
                  tData.status = 'debating'
              }

              const val = result.validation?.final_recommendation
              if (val) {
                  tData.validator = `${val.decision} (${val.conviction})`
                  tData.status = 'validated'
              }
          }
          newState[ticker] = tData
          return newState
      })
  }

  const parseLog = (log: string) => {
    if (log.includes('C=$')) updateSimpleNode('producers', 'active', `Received ${log.split(':')[0].replace(/.*📊/, '')}`)
    if (log.includes('Aggregated')) updateSimpleNode('engine', 'active', `Processing Batch`)
    if (log.includes('Synced')) updateSimpleNode('mongodb', 'success', 'Synced')
    if (log.includes('Waiting')) updateSimpleNode('finrl', 'loading', 'Accumulating Data...')
    if (log.includes('FinRL:')) {
        const count = log.match(/(\d+) tickers/)?.[1] || '0'
        updateSimpleNode('finrl', 'success', `Selected ${count} Tickers`)
    }
  }

  const updateSimpleNode = (nodeId: string, status: string, msg: string) => {
    if (['producers', 'engine', 'mongodb', 'finrl', 'execution'].includes(nodeId)) {
      setNodes(nds => nds.map(n => {
        if (n.id === nodeId) return { ...n, data: { ...n.data, status, liveMsg: msg } }
        return n
      }))
    }
  }

  // Sync state to Custom Nodes
  useEffect(() => {
    setNodes(nds => nds.map(node => {
      if (node.id === 'consensus') return { ...node, data: { ...node.data, tickers: activeTickers } }
      if (node.id === 'execution') return { ...node, data: { ...node.data, trades: tradeDecisions } }
      return node
    }))
  }, [activeTickers, tradeDecisions, setNodes])

  return (
    <div className="flex flex-col h-screen max-h-screen bg-slate-950 text-white overflow-hidden">
       {/* HEADER */}
       <div className="h-14 flex justify-between items-center px-6 border-b border-slate-800 bg-slate-900 shrink-0 z-50">
           <div className="flex items-center gap-3">
               <div className="w-2.5 h-2.5 rounded-full bg-emerald-500 animate-pulse shadow-[0_0_10px_#10b981]" />
               <div>
                  <h1 className="text-lg font-bold tracking-tight text-white">AEGIS OPERATIONS CENTER</h1>
                  <p className="text-[10px] text-slate-400 font-mono">AUTONOMOUS TRADING PIPELINE</p>
               </div>
           </div>
           <div className="px-3 py-1 bg-black/40 rounded border border-slate-800 text-xs font-mono text-slate-400 flex items-center gap-2">
              <Activity size={14} className="text-emerald-500" /> SYSTEM ACTIVE
           </div>
       </div>

       {/* MAIN AREA */}
       <div className="flex-1 flex min-h-0">
          {/* GRAPH AREA */}
          <div className="flex-1 relative bg-slate-950 flex flex-col min-w-0">
             <ReactFlow 
                nodes={nodes} 
                edges={edges} 
                nodeTypes={nodeTypes} 
                fitView 
                minZoom={0.5}
                className="bg-slate-950 w-full h-full"
             >
                <Background color="#334155" gap={32} size={1} />
                <Controls className="bg-slate-800 border-slate-700 fill-white" />
             </ReactFlow>
          </div>

          {/* SIDEBAR */}
          <div className="w-[380px] bg-black border-l border-slate-800 flex flex-col shrink-0 z-10 shadow-xl">
              <div className="p-3 bg-slate-900/80 backdrop-blur border-b border-slate-800 flex items-center gap-2 text-xs font-bold text-slate-300">
                  <Terminal size={14} /> LIVE SYSTEM LOGS
              </div>
              <div className="flex-1 overflow-y-auto p-3 font-mono text-[10px] space-y-1">
                  {logs.map((l, i) => (
                     <div key={i} className={`break-words border-l-2 pl-2 py-0.5 leading-tight ${l.includes('ERROR') ? 'border-red-500 text-red-400 bg-red-900/10' : l.includes('SUCCESS') ? 'border-emerald-500 text-emerald-400' : l.includes('WARNING') ? 'border-amber-500 text-amber-400' : 'border-slate-700 text-slate-500'}`}>
                         {l.replace('[PIPE]', '')}
                     </div>
                  ))}
                  <div ref={terminalEndRef} />
              </div>
          </div>
       </div>
    </div>
  )
}