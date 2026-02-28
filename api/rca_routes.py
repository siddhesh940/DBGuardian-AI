import fastapi
import fastapi.responses
import os
import json
import shutil
import typing
import numpy as np

import engine.rca_engine
import engine.time_window_detector
import engine.dba_formatter

import api.auth_routes

router = fastapi.APIRouter()

# =====================================================
# AUTH
# =====================================================
def get_username(request: fastapi.Request) -> str:
    session_id: typing.Optional[str] = request.cookies.get("session_id")
    if not session_id or session_id not in api.auth_routes.sessions:
        raise fastapi.HTTPException(status_code=401, detail="Unauthorized user")
    return api.auth_routes.sessions[session_id]["username"]

# =====================================================
# USER PATHS
# =====================================================
def get_user_paths(username: str) -> typing.Dict[str, str]:
    base: str = f"data/users/{username}"
    paths: typing.Dict[str, str] = {
        "base": base,
        "parsed_csv": f"{base}/parsed_csv",
        "raw_html": f"{base}/raw_html",
        "results_dir": f"{base}/results",
        "result_file": f"{base}/results/rca_result.json",
    }

    for p in paths.values():  # noqa
        if not p.endswith(".json"):
            os.makedirs(p, exist_ok=True)

    return paths

# =====================================================
# JSON CLEANER
# =====================================================
def clean_nan_values(obj: typing.Any) -> typing.Any:
    if isinstance(obj, dict):
        return {k: clean_nan_values(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_nan_values(v) for v in obj]
    if isinstance(obj, float):
        if np.isnan(obj) or np.isinf(obj):
            return 0.0
    return obj

# =====================================================
# FIND AWR HTML FILE
# =====================================================
def _find_awr_html_file(raw_html_dir: str) -> typing.Optional[str]:
    """
    Find the most recently modified AWR HTML file in the raw_html directory.
    Returns None if no AWR file is found.
    """
    if not os.path.exists(raw_html_dir):
        return None
    
    awr_files = []
    try:
        for filename in os.listdir(raw_html_dir):  # noqa
            if not filename.lower().endswith('.html'):
                continue
            filepath: str = os.path.join(raw_html_dir, filename)
            
            # Check if it's an AWR file by name or content
            if 'awr' in filename.lower():
                awr_files.append((filepath, os.path.getmtime(filepath)))
            else:
                # Quick content check for AWR Report signature
                try:
                    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                        sample: str = f.read(5000)
                    if 'AWR Report' in sample or 'Begin Snap' in sample:
                        awr_files.append((filepath, os.path.getmtime(filepath)))
                except:
                    pass
    except Exception as e:
        print(f"Error finding AWR HTML file: {e}")
        return None
    
    if not awr_files:
        return None
    
    # Return the most recently modified AWR file
    awr_files.sort(key=lambda x: x[1], reverse=True)
    return awr_files[0][0]

# =====================================================
# UPLOAD + PARSE (🔥 CRITICAL ROUTE)
# =====================================================
@router.post("/upload")
async def upload_files(
    request: fastapi.Request,
    files: typing.List[fastapi.UploadFile] = fastapi.File(...)
) -> fastapi.responses.JSONResponse:

    username: str = get_username(request)
    paths: typing.Dict[str, str] = get_user_paths(username)

    parsed_dir: str = paths["parsed_csv"]
    upload_dir: str = paths["raw_html"]

    # 🔥 ALWAYS RESET OLD DATA (both CSV and HTML folders)
    if os.path.exists(parsed_dir):
        shutil.rmtree(parsed_dir)
    if os.path.exists(upload_dir):
        shutil.rmtree(upload_dir)  # Clear old HTML files too!
    os.makedirs(parsed_dir, exist_ok=True)
    os.makedirs(upload_dir, exist_ok=True)

    uploaded_files = []
    parsing_results = []
    csv_generated = []

    for file in files:  # noqa
        if not file.filename.lower().endswith(".html"):
            continue

        file_path: str = os.path.join(upload_dir, file.filename)

        with open(file_path, "wb") as buffer:
            buffer.write(await file.read())

        uploaded_files.append(file.filename)
        base = os.path.splitext(file.filename)[0]

        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                sample: str = f.read(5000)

            if "awr" in file.filename.lower() or "AWR Report" in sample:
                from parsers.awr_html_parser import parse_awr_with_prefix
                generated = parse_awr_with_prefix(file_path, base, parsed_dir)
                ftype = "AWR"

            elif "ash" in file.filename.lower() or "ASH Report" in sample:
                from parsers.ash_html_parser import parse_ash_with_prefix
                generated = parse_ash_with_prefix(file_path, base, parsed_dir)
                ftype = "ASH"

            else:
                raise Exception("Unknown report type")

            csv_generated.extend(generated)

            parsing_results.append({
                "file": file.filename,
                "type": ftype,
                "status": "success",
                "csv_files": len(generated),
                "csv_list": generated
            })

        except Exception as e:
            parsing_results.append({
                "file": file.filename,
                "status": "error",
                "error": str(e),
                "csv_files": 0,
                "csv_list": []
            })

    existing_csvs: list[str] = [f for f in os.listdir(parsed_dir) if f.endswith(".csv")]

    # ===== UNIFIED METRICS (Single Source of Truth) =====
    unified_metrics = None
    html_file = None
    try:
        from engine.unified_metrics import UnifiedMetricsCalculator, clear_metrics_cache
        # Clear any cached metrics to ensure fresh calculation
        clear_metrics_cache(parsed_dir)
        
        html_file: str | None = _find_awr_html_file(upload_dir)
        if html_file:
            metrics_calc = UnifiedMetricsCalculator(parsed_dir, html_file)
            unified_metrics = metrics_calc.get_metrics_dict()
    except Exception as e:
        print(f"Warning: Could not compute unified metrics: {e}")

    try:
        detector = engine.time_window_detector.TimeWindowDetector(parsed_dir, html_file)  # Pass HTML file for correct CPU
        high_load = detector.detect_high_load_periods()
        
        # =====================================================
        # INJECT UNIFIED ELAPSED TIME INTO HIGH LOAD PERIODS
        # =====================================================
        # Ensures High Load Detection shows the SAME elapsed time as everywhere else
        if unified_metrics and high_load:
            authoritative_elapsed = unified_metrics.get("total_elapsed_time_s", 0)
            for period in high_load:
                if isinstance(period, dict) and "metrics" in period:
                    period["metrics"]["total_elapsed_time_s"] = authoritative_elapsed
    except Exception:
        high_load = []

    response_data = {
        "status": "success",
        "uploaded_files": uploaded_files,
        "parsing_results": parsing_results,
        "total_csv_files": len(existing_csvs),
        "new_csv_files_generated": len(csv_generated),
        "csv_file_list": existing_csvs,
        "high_load_periods": high_load
    }
    
    # Include unified metrics in response (for UI consumption)
    if unified_metrics:
        response_data["unified_metrics"] = unified_metrics
    
    return fastapi.responses.JSONResponse(response_data)

# =====================================================
# RUN RCA
# =====================================================
@router.post("/run_rca")
async def run_rca(request: fastapi.Request) -> fastapi.responses.JSONResponse:
    try:
        username: str = get_username(request)
        paths: typing.Dict[str, str] = get_user_paths(username)

        body = await request.json()

        time_filter: typing.Dict[str, typing.Any] = {
            "start_hour": body.get("start_hour"),
            "start_minute": body.get("start_minute"),
            "end_hour": body.get("end_hour"),
            "end_minute": body.get("end_minute"),
            "time_window": body.get("time_window", "Custom Time Range")
        }
        
        # Find the AWR HTML file for authoritative time extraction
        html_file_path: str | None = _find_awr_html_file(paths["raw_html"])

        rca_eng = engine.rca_engine.RCAEngine(
            csv_dir=paths["parsed_csv"],
            time_filter=time_filter,
            username=username,
            html_file_path=html_file_path
        )

        rca_result = rca_eng.run()
        # Note: analysis_window is now set by RCAEngine using authoritative metadata
        # Only override if the engine didn't set it
        if not rca_result.get("analysis_window") or rca_result["analysis_window"] == "--":
            rca_result["analysis_window"] = time_filter["time_window"]
        rca_result["time_filter"] = time_filter

        if "dba_expert_analysis" in rca_result:
            rca_result["dba_expert_analysis"] = engine.dba_formatter.DBAFormatter.format_for_api(
                rca_result["dba_expert_analysis"]
            )

        # =====================================================
        # UNIFIED ELAPSED TIME (Single Source of Truth)
        # =====================================================
        try:
            from engine.unified_metrics import UnifiedMetricsCalculator
            metrics_calc = UnifiedMetricsCalculator(paths["parsed_csv"], html_file_path)
            unified: engine.unified_metrics.AWRMetrics = metrics_calc.compute_metrics()
            
            if unified.is_valid:
                authoritative_elapsed: float = round(unified.total_elapsed_time_s, 2)
                rca_result["unified_metrics"] = metrics_calc.get_metrics_dict()
                if "dba_expert_analysis" in rca_result:
                    ws = rca_result["dba_expert_analysis"].get("workload_summary", {})
                    if ws:
                        ws["total_elapsed_s"] = authoritative_elapsed
        except Exception as e:
            print(f"Warning: Could not inject unified elapsed time: {e}")

        try:
            import agent.sql_agent
            sql_agent_inst = agent.sql_agent.SQLAgent(rca_result)
            rca_result["agent_insights"] = sql_agent_inst.explain()
        except Exception as e:
            rca_result["agent_insights"] = [f"Agent failed: {e}"]

        cleaned = clean_nan_values(rca_result)

        with open(paths["result_file"], "w", encoding="utf-8") as f:
            json.dump(cleaned, f, indent=2)

        return fastapi.responses.JSONResponse(cleaned)

    except fastapi.HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return fastapi.responses.JSONResponse(
            {"detail": f"RCA analysis failed: {type(e).__name__}: {str(e)}"},
            status_code=500
        )

# =====================================================
# RESULTS
# =====================================================
@router.get("/results")
async def get_results(request: fastapi.Request) -> fastapi.responses.JSONResponse:
    username: str = get_username(request)
    paths: typing.Dict[str, str] = get_user_paths(username)

    parsed_dir: str = paths["parsed_csv"]
    result_file: str = paths["result_file"]

    response: typing.Dict[str, bool] = {
        "has_data": False,
        "has_parsed_csv": False,
        "has_rca_result": False
    }

    # Count raw HTML reports uploaded
    html_files: list[str] = [f for f in os.listdir(paths["raw_html"]) if f.endswith(".html")] \
        if os.path.exists(paths["raw_html"]) else []
    response["html_count"] = len(html_files)
    response["html_file_list"] = html_files

    csv_files: list[str] = [f for f in os.listdir(parsed_dir) if f.endswith(".csv")] \
        if os.path.exists(parsed_dir) else []

    if csv_files:
        response["has_parsed_csv"] = True
        response["csv_count"] = len(csv_files)
        response["csv_file_list"] = csv_files

        # ===== UNIFIED METRICS (Single Source of Truth) =====
        unified_metrics = None
        html_file = None
        try:
            from engine.unified_metrics import UnifiedMetricsCalculator, clear_metrics_cache
            # Clear any cached metrics to ensure fresh calculation
            clear_metrics_cache(parsed_dir)
            
            html_file: str | None = _find_awr_html_file(paths["raw_html"])
            if html_file:
                metrics_calc = UnifiedMetricsCalculator(parsed_dir, html_file)
                unified_metrics = metrics_calc.get_metrics_dict()
                response["unified_metrics"] = unified_metrics
        except Exception as e:
            print(f"Warning: Could not compute unified metrics: {e}")

        try:
            detector = engine.time_window_detector.TimeWindowDetector(parsed_dir, html_file)  # Pass HTML file for correct CPU
            high_load = detector.detect_high_load_periods()
            
            # =====================================================
            # INJECT UNIFIED ELAPSED TIME INTO HIGH LOAD PERIODS
            # =====================================================
            # Ensures High Load Detection shows the SAME elapsed time as everywhere else
            if unified_metrics and high_load:
                authoritative_elapsed = unified_metrics.get("total_elapsed_time_s", 0)
                for period in high_load:
                    if isinstance(period, dict) and "metrics" in period:
                        period["metrics"]["total_elapsed_time_s"] = authoritative_elapsed
            
            response["high_load_periods"] = high_load
        except Exception:
            response["high_load_periods"] = []

    if os.path.exists(result_file):
        response["has_rca_result"] = True
        with open(result_file, "r", encoding="utf-8") as f:
            response["analysis_results"] = json.load(f)

    response["has_data"] = response["has_parsed_csv"] or response["has_rca_result"]
    response["username"] = username

    if not response["has_data"]:
        return fastapi.responses.JSONResponse({"error": "No existing data found", "username": username}, status_code=404)

    return fastapi.responses.JSONResponse(response)

# =====================================================
# RECOMMEND
# =====================================================
@router.post("/recommend")
async def get_fix_recommendations(
    request: fastapi.Request,
    request_data: typing.Dict[str, typing.Any]
) -> fastapi.responses.JSONResponse:

    get_username(request)

    sql_id: typing.Any | None = request_data.get("sql_id")
    query_data: typing.Any | None = request_data.get("query_data")

    if not sql_id:
        raise fastapi.HTTPException(status_code=400, detail="SQL ID required")

    import agent.sql_agent
    sql_agent_inst = agent.sql_agent.SQLAgent()
    recommendations = await sql_agent_inst.generate_fix_recommendations(sql_id, query_data)
    return fastapi.responses.JSONResponse(recommendations)

