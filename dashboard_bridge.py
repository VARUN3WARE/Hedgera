
# import asyncio
# import sys
# import re
# import os
# import json
# import glob
# from pathlib import Path
# from typing import List
# from fastapi import FastAPI, WebSocket, WebSocketDisconnect
# from fastapi.middleware.cors import CORSMiddleware
# from dotenv import load_dotenv

# load_dotenv()

# app = FastAPI()

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# connected_clients: List[WebSocket] = []
# process = None

# # --- HELPER: FIND LATEST LOG FOLDER ---
# def get_latest_log_dir():
#     """Finds the most recently created cycle folder in agent_logs"""
#     try:
#         # 1. Find the latest session folder (enhanced_YYYYMMDD_HHMMSS)
#         base_path = Path("agent_logs")
#         if not base_path.exists(): return None
        
#         sessions = [f for f in base_path.iterdir() if f.is_dir() and "enhanced_" in f.name]
#         if not sessions: return None
        
#         latest_session = max(sessions, key=os.path.getmtime)
        
#         # 2. Find the latest cycle folder inside that session (cycle_XX_...)
#         cycles = [f for f in latest_session.iterdir() if f.is_dir() and "cycle_" in f.name]
        
#         # If no cycle folder yet, return the session folder (sometimes logs are there)
#         if not cycles: return latest_session
        
#         return max(cycles, key=os.path.getmtime)
#     except Exception as e:
#         print(f"Error finding log dir: {e}")
#         return None

# # --- LOG PARSING PATTERNS ---
# LOG_HANDLERS = [
#     (r"Starting PriceProducer", {"node": "producers", "status": "active", "msg": "Streaming Prices"}),
#     (r"PriceProducer: Published", {"node": "producers", "status": "success", "msg": "Data Published"}),
#     (r"Fetched .* symbols", {"node": "producers", "status": "active", "msg": "Fetching"}),
#     (r"MongoDB Sync Service initialized", {"node": "mongodb", "status": "active", "msg": "Ready"}),
#     (r"Synced .* records", {"node": "mongodb", "status": "success", "msg": "Synced"}),
#     (r"Initializing Streaming Engine", {"node": "engine", "status": "loading", "msg": "Starting"}),
#     (r"Publish interval reached", {"node": "engine", "status": "pulsing", "msg": "Publishing"}),
#     (r"Aggregated data: .* symbols", {"node": "engine", "status": "active", "msg": "Processing"}),
#     (r"FinRL Cycle Start", {"node": "finrl", "status": "loading", "msg": "Analyzing"}),
#     (r"FinRL output from Redis", {"node": "finrl", "status": "success", "msg": "Done"}),
#     (r"Waiting\.\.\. .* remaining", {"node": "finrl", "status": "loading", "msg": "Waiting"}),
#     (r"Running News Analyst", {"node": "analysts", "status": "active", "msg": "Analyzing News"}),
#     (r"Running Social Analyst", {"node": "analysts", "status": "active", "msg": "Analyzing Social"}),
#     (r"Running Market Analyst", {"node": "analysts", "status": "active", "msg": "Analyzing Market"}),
#     (r"Running SEC Analyst", {"node": "analysts", "status": "active", "msg": "Analyzing SEC"}),
#     (r"RUNNING DEBATE", {"node": "debate", "status": "active", "msg": "Debating"}),
#     (r"EXECUTING TRADES", {"node": "execution", "status": "active", "msg": "Trading"}),
#     (r"Trades Executed", {"node": "execution", "status": "success", "msg": "Filled"}),
# ]

# def parse_log_line(line):
#     clean_line = line.strip()
#     update = None
#     for pattern, action in LOG_HANDLERS:
#         if re.search(pattern, clean_line):
#             update = action
#             break
#     return {"type": "log", "raw": clean_line, "update": update}

# async def broadcast(message: dict):
#     for client in connected_clients:
#         try:
#             await client.send_json(message)
#         except:
#             pass

# async def run_pipeline_script():
#     global process
    
#     if not os.path.exists("parallel_full_pipeline_clean.py"):
#         await broadcast({"type": "log", "raw": "❌ ERROR: parallel_full_pipeline_clean.py not found!"})
#         return

#     env = os.environ.copy()
#     env["PYTHONIOENCODING"] = "utf-8"
    
#     cmd = [sys.executable, "-u", "-X", "utf8", "parallel_full_pipeline_clean.py", "--quick"]
    
#     print(f"🚀 Launching: {' '.join(cmd)}")
#     await broadcast({"type": "log", "raw": "🚀 System Launching..."})
    
#     try:
#         process = await asyncio.create_subprocess_exec(
#             *cmd,
#             stdout=asyncio.subprocess.PIPE,
#             stderr=asyncio.subprocess.STDOUT, 
#             env=env 
#         )

#         while True:
#             line = await process.stdout.readline()
#             if not line:
#                 break
            
#             decoded_line = line.decode('utf-8', errors='replace').rstrip()
#             print(f"[PIPE] {decoded_line}") 
            
#             # --- FILE WATCHER LOGIC ---
#             # When we see "Saved X.json", we actively go find it
#             if "💾 Saved" in decoded_line and ".json" in decoded_line:
#                 try:
#                     filename = decoded_line.split("Saved")[1].strip()
                    
#                     # 1. Get the latest folder (Robust Method)
#                     log_dir = get_latest_log_dir()
                    
#                     if log_dir:
#                         file_path = log_dir / filename
#                         # Wait briefly for file write to complete
#                         await asyncio.sleep(0.5) 
                        
#                         if file_path.exists():
#                             with open(file_path, 'r', encoding='utf-8') as f:
#                                 json_data = json.load(f)
                            
#                             # Send to frontend
#                             await broadcast({
#                                 "type": "file_update",
#                                 "filename": filename,
#                                 "data": json_data
#                             })
#                             print(f"✅ Served JSON: {filename}")
#                         else:
#                             print(f"⚠️ File not found at: {file_path}")
#                 except Exception as e:
#                     print(f"❌ JSON Read Error: {e}")

#             parsed_data = parse_log_line(decoded_line)
#             await broadcast(parsed_data)

#         return_code = await process.wait()
#         await broadcast({"type": "system", "status": "stopped", "code": return_code})
        
#     except Exception as e:
#         print(f"Error: {e}")
#         await broadcast({"type": "log", "raw": f"❌ Execution Error: {e}"})

# @app.websocket("/ws/logs")
# async def websocket_endpoint(websocket: WebSocket):
#     await websocket.accept()
#     connected_clients.append(websocket)
    
#     try:
#         while True:
#             data = await websocket.receive_text()
#             if data == "START":
#                 if process is None or process.returncode is not None:
#                     asyncio.create_task(run_pipeline_script())
#                     await websocket.send_json({"type": "system", "status": "started"})
#             elif data == "STOP":
#                 if process:
#                     process.terminate()
#                     await websocket.send_json({"type": "system", "status": "stopping"})
#     except WebSocketDisconnect:
#         connected_clients.remove(websocket)


import asyncio
import sys
import re
import os
import json
import glob
from pathlib import Path
from typing import List, Dict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

connected_clients: List[WebSocket] = []
process = None
watcher_task = None
sent_files_cache: Dict[str, float] = {} # Tracks filename -> last_modified_time

# --- HELPER: FIND LATEST PARALLEL LOG FOLDER ---
def get_latest_log_dir():
    """Finds the most recently created parallel session in agent_logs"""
    try:
        base_path = Path("agent_logs")
        if not base_path.exists(): return None
        
        # Look for "parallel_" or "enhanced_" folders
        sessions = [f for f in base_path.iterdir() if f.is_dir() and ("parallel_" in f.name or "enhanced_" in f.name)]
        if not sessions: return None
        
        latest_session = max(sessions, key=os.path.getmtime)
        
        # Find latest cycle inside
        cycles = [f for f in latest_session.iterdir() if f.is_dir() and "cycle_" in f.name]
        if not cycles: return latest_session
        
        return max(cycles, key=os.path.getmtime)
    except Exception as e:
        print(f"Error finding log dir: {e}")
        return None

# --- LOG PARSING PATTERNS ---
LOG_HANDLERS = [
    (r"Starting PriceProducer", {"node": "producers", "status": "active", "msg": "Streaming Prices"}),
    (r"PriceProducer: Published", {"node": "producers", "status": "success", "msg": "Data Published"}),
    (r"Fetched .* symbols", {"node": "producers", "status": "active", "msg": "Fetching"}),
    (r"MongoDB Sync Service initialized", {"node": "mongodb", "status": "active", "msg": "Ready"}),
    (r"Synced .* records", {"node": "mongodb", "status": "success", "msg": "Synced"}),
    (r"Initializing Streaming Engine", {"node": "engine", "status": "loading", "msg": "Starting"}),
    (r"Publish interval reached", {"node": "engine", "status": "pulsing", "msg": "Publishing"}),
    (r"Aggregated data: .* symbols", {"node": "engine", "status": "active", "msg": "Processing"}),
    (r"FinRL Cycle Start", {"node": "finrl", "status": "loading", "msg": "Analyzing"}),
    (r"FinRL output from Redis", {"node": "finrl", "status": "success", "msg": "Done"}),
    (r"Waiting\.\.\. .* remaining", {"node": "finrl", "status": "loading", "msg": "Waiting"}),
    (r"Running News Analyst", {"node": "analysts", "status": "active", "msg": "Analyzing News"}),
    (r"Running Social Analyst", {"node": "analysts", "status": "active", "msg": "Analyzing Social"}),
    (r"Running Market Analyst", {"node": "analysts", "status": "active", "msg": "Analyzing Market"}),
    (r"Running SEC Analyst", {"node": "analysts", "status": "active", "msg": "Analyzing SEC"}),
    (r"RUNNING DEBATE", {"node": "debate", "status": "active", "msg": "Debating"}),
    (r"EXECUTING TRADES", {"node": "execution", "status": "active", "msg": "Trading"}),
    (r"Trades Executed", {"node": "execution", "status": "success", "msg": "Filled"}),
]

def parse_log_line(line):
    clean_line = line.strip()
    update = None
    for pattern, action in LOG_HANDLERS:
        if re.search(pattern, clean_line):
            update = action
            break
    return {"type": "log", "raw": clean_line, "update": update}

async def broadcast(message: dict):
    for client in connected_clients:
        try:
            await client.send_json(message)
        except:
            pass

# --- INDEPENDENT FILE WATCHER ---
async def watch_files_loop():
    """Background task to scan for new JSON files every 2 seconds"""
    print("👀 File Watcher Started")
    while True:
        try:
            log_dir = get_latest_log_dir()
            if log_dir:
                # Scan for all relevant JSON files
                files = list(log_dir.glob("*.json"))
                
                for file_path in files:
                    try:
                        mtime = os.path.getmtime(file_path)
                        filename = file_path.name
                        
                        # Check if file is new or modified
                        if filename not in sent_files_cache or sent_files_cache[filename] < mtime:
                            # It's updated! Read and send.
                            async with asyncio.Lock():
                                with open(file_path, 'r', encoding='utf-8') as f:
                                    # Ensure file isn't empty/corrupt from write in progress
                                    content = f.read()
                                    if content.strip():
                                        data = json.loads(content)
                                        await broadcast({
                                            "type": "file_update",
                                            "filename": filename,
                                            "data": data
                                        })
                                        print(f"📤 Auto-Sent: {filename}")
                                        sent_files_cache[filename] = mtime
                    except Exception as e:
                        # File might be locked or writing, skip and retry next loop
                        pass
                        
            await asyncio.sleep(2) # Scan every 2 seconds
        except asyncio.CancelledError:
            print("👀 File Watcher Stopped")
            break
        except Exception as e:
            print(f"Watcher Error: {e}")
            await asyncio.sleep(5)

async def run_pipeline_script():
    global process, watcher_task, sent_files_cache
    
    # Reset cache on new run
    sent_files_cache = {}
    
    script_name = "parallel_full_pipeline_clean.py"
    if not os.path.exists(script_name):
        script_name = "full_pipeline_enhanced.py"
        if not os.path.exists(script_name):
             await broadcast({"type": "log", "raw": "❌ ERROR: No pipeline script found!"})
             return

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    
    cmd = [sys.executable, "-u", "-X", "utf8", script_name, "--quick"]
    
    print(f"🚀 Launching: {' '.join(cmd)}")
    await broadcast({"type": "log", "raw": f"🚀 Launching {script_name}..."})
    
    # START FILE WATCHER
    if watcher_task is None or watcher_task.done():
        watcher_task = asyncio.create_task(watch_files_loop())
    
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT, 
            env=env 
        )

        while True:
            line = await process.stdout.readline()
            if not line: break
            
            decoded_line = line.decode('utf-8', errors='replace').rstrip()
            print(f"[PIPE] {decoded_line}") 
            
            # Send raw log for terminal & status updates
            await broadcast(parse_log_line(decoded_line))

        return_code = await process.wait()
        await broadcast({"type": "system", "status": "stopped", "code": return_code})
        
    except Exception as e:
        print(f"Error: {e}")
        await broadcast({"type": "log", "raw": f"❌ Execution Error: {e}"})
    finally:
        # Stop watcher when pipeline stops
        if watcher_task:
            watcher_task.cancel()

@app.websocket("/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "START":
                if process is None or process.returncode is not None:
                    asyncio.create_task(run_pipeline_script())
                    await websocket.send_json({"type": "system", "status": "started"})
            elif data == "STOP":
                if process:
                    process.terminate()
                    await websocket.send_json({"type": "system", "status": "stopping"})
    except WebSocketDisconnect:
        connected_clients.remove(websocket)