import runpod
from runpod.serverless.utils import rp_upload
import os
import websocket
import base64
import json
import uuid
import logging
import urllib.request
import urllib.parse
import binascii
import subprocess
import time
import random

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

server_address = os.getenv('SERVER_ADDRESS', '127.0.0.1')
client_id = str(uuid.uuid4())

def to_nearest_multiple_of_16(value):
    """주어진 값을 가장 가까운 16의 배수로 보정, 최소 16 보장"""
    try:
        numeric_value = float(value)
    except Exception:
        raise Exception(f"width/height 값이 숫자가 아닙니다: {value}")
    adjusted = int(round(numeric_value / 16.0) * 16)
    if adjusted < 16:
        adjusted = 16
    return adjusted

def process_input(input_data, temp_dir, output_filename, input_type):
    """입력 데이터를 처리하여 파일 경로를 반환하는 함수"""
    if input_type == "path":
        logger.info(f"📁 경로 입력 처리: {input_data}")
        return input_data
    elif input_type == "url":
        logger.info(f"🌐 URL 입력 처리: {input_data}")
        os.makedirs(temp_dir, exist_ok=True)
        file_path = os.path.abspath(os.path.join(temp_dir, output_filename))
        return download_file_from_url(input_data, file_path)
    elif input_type == "base64":
        logger.info(f"🔢 Base64 입력 처리")
        return save_base64_to_file(input_data, temp_dir, output_filename)
    else:
        raise Exception(f"지원하지 않는 입력 타입: {input_type}")

def download_file_from_url(url, output_path):
    """URL에서 파일을 다운로드하는 함수"""
    try:
        result = subprocess.run(
            ['wget', '-O', output_path, '--no-verbose', url],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            logger.info(f"✅ URL에서 파일을 성공적으로 다운로드했습니다: {url} -> {output_path}")
            return output_path
        else:
            logger.error(f"❌ wget 다운로드 실패: {result.stderr}")
            raise Exception(f"URL 다운로드 실패: {result.stderr}")
    except subprocess.TimeoutExpired:
        logger.error("❌ 다운로드 시간 초과")
        raise Exception("다운로드 시간 초과")
    except Exception as e:
        logger.error(f"❌ 다운로드 중 오류 발생: {e}")
        raise Exception(f"다운로드 중 오류 발생: {e}")

def save_base64_to_file(base64_data, temp_dir, output_filename):
    """Base64 데이터를 파일로 저장하는 함수"""
    try:
        decoded_data = base64.b64decode(base64_data)
        os.makedirs(temp_dir, exist_ok=True)
        file_path = os.path.abspath(os.path.join(temp_dir, output_filename))
        with open(file_path, 'wb') as f:
            f.write(decoded_data)
        logger.info(f"✅ Base64 입력을 '{file_path}' 파일로 저장했습니다.")
        return file_path
    except (binascii.Error, ValueError) as e:
        logger.error(f"❌ Base64 디코딩 실패: {e}")
        raise Exception(f"Base64 디코딩 실패: {e}")

def queue_prompt(prompt):
    url = f"http://{server_address}:8188/prompt"
    logger.info(f"Queueing prompt to: {url}")
    p = {"prompt": prompt, "client_id": client_id}
    data = json.dumps(p).encode('utf-8')
    req = urllib.request.Request(url, data=data)
    return json.loads(urllib.request.urlopen(req).read())

def get_history(prompt_id):
    url = f"http://{server_address}:8188/history/{prompt_id}"
    logger.info(f"Getting history from: {url}")
    with urllib.request.urlopen(url) as response:
        return json.loads(response.read())

def get_videos(ws, prompt):
    prompt_id = queue_prompt(prompt)['prompt_id']
    output_videos = {}

    while True:
        out = ws.recv()
        if isinstance(out, str):
            message = json.loads(out)
            if message.get('type') == 'executing':
                data = message.get('data', {})
                if data.get('node') is None and data.get('prompt_id') == prompt_id:
                    break
        else:
            continue

    history = get_history(prompt_id)[prompt_id]
    for node_id in history.get('outputs', {}):
        node_output = history['outputs'][node_id]
        videos_output = []
        if 'gifs' in node_output:
            for video in node_output['gifs']:
                with open(video['fullpath'], 'rb') as f:
                    video_data = base64.b64encode(f.read()).decode('utf-8')
                videos_output.append(video_data)
        output_videos[node_id] = videos_output

    return output_videos

def load_workflow(workflow_path):
    with open(workflow_path, 'r') as file:
        return json.load(file)

def handler(job):
    job_input = job.get("input", {}) or {}
    logger.info(f"Received job input: {job_input}")

    task_id = f"task_{uuid.uuid4()}"
    temp_dir = os.path.join("/tmp", "runpod_inputs", task_id)
    os.makedirs(temp_dir, exist_ok=True)

    # -----------------------------
    # 1) PROMPT (REQUIRED)
    # -----------------------------
    user_prompt = job_input.get("prompt")
    if not user_prompt or not isinstance(user_prompt, str):
        raise Exception("Missing required input: 'prompt' (string)")

    negative_prompt = job_input.get(
        "negative_prompt",
        "bright tones, overexposed, static, blurred details, subtitles, style, works, paintings, images, static, overall gray, worst quality, low quality, JPEG compression residue, ugly, incomplete, extra fingers, poorly drawn hands, poorly drawn faces, deformed, disfigured, misshapen limbs, fused fingers, still picture, messy background, three legs, many people in the background, walking backwards"
    )

    # -----------------------------
    # 2) IMAGE INPUT (SUPPORTS RunPod 'images' ARRAY)
    # -----------------------------
    image_path = None

    # Preferred: RunPod style: images: [{name, url}, ...]
    images_arr = job_input.get("images")
    if isinstance(images_arr, list) and len(images_arr) > 0:
        first = images_arr[0] or {}
        url = first.get("url")
        if url:
            image_path = process_input(url, temp_dir, "input_0.png", "url")

    # Back-compat: image_path / image_url / image_base64
    if not image_path:
        if "image_path" in job_input:
            image_path = process_input(job_input["image_path"], temp_dir, "input_image.jpg", "path")
        elif "image_url" in job_input:
            image_path = process_input(job_input["image_url"], temp_dir, "input_image.jpg", "url")
        elif "image_base64" in job_input:
            image_path = process_input(job_input["image_base64"], temp_dir, "input_image.jpg", "base64")

    if not image_path:
        image_path = "/example_image.png"
        logger.info("기본 이미지 파일을 사용합니다: /example_image.png")

    # Optional end image (FLF2V)
    end_image_path_local = None
    if "end_image_path" in job_input:
        end_image_path_local = process_input(job_input["end_image_path"], temp_dir, "end_image.jpg", "path")
    elif "end_image_url" in job_input:
        end_image_path_local = process_input(job_input["end_image_url"], temp_dir, "end_image.jpg", "url")
    elif "end_image_base64" in job_input:
        end_image_path_local = process_input(job_input["end_image_base64"], temp_dir, "end_image.jpg", "base64")

    # -----------------------------
    # 3) DEFAULTS (NO KeyError)
    # -----------------------------
    seed = job_input.get("seed", -1)
    if isinstance(seed, str) and seed.strip().lstrip("-").isdigit():
        seed = int(seed.strip())
    if not isinstance(seed, int):
        seed = -1
    if seed == -1:
        seed = random.randint(0, 2**31 - 1)

    cfg = job_input.get("cfg", 7)
    try:
        cfg = float(cfg)
    except Exception:
        cfg = 7.0

    length = job_input.get("length", 81)
    try:
        length = int(length)
    except Exception:
        length = 81

    steps = job_input.get("steps", 10)
    try:
        steps = int(steps)
    except Exception:
        steps = 10

    # Reasonable defaults for WAN i2v
    original_width = job_input.get("width", 832)
    original_height = job_input.get("height", 480)

    adjusted_width = to_nearest_multiple_of_16(original_width)
    adjusted_height = to_nearest_multiple_of_16(original_height)

    context_overlap = job_input.get("context_overlap", 48)
    try:
        context_overlap = int(context_overlap)
    except Exception:
        context_overlap = 48

    # LoRA pairs (optional)
    lora_pairs = job_input.get("lora_pairs", [])
    if not isinstance(lora_pairs, list):
        lora_pairs = []
    lora_pairs = lora_pairs[:4]
    lora_count = len(lora_pairs)

    # -----------------------------
    # 4) LOAD WORKFLOW + INJECT
    # -----------------------------
    workflow_file = "/new_Wan22_flf2v_api.json" if end_image_path_local else "/new_Wan22_api.json"
    logger.info(f"Using {'FLF2V' if end_image_path_local else 'single'} workflow with {lora_count} LoRA pairs")
    prompt = load_workflow(workflow_file)

    # Required nodes exist?
    for nid in ["244", "541", "135", "220", "540", "235", "236", "498"]:
        if nid not in prompt:
            raise Exception(f"Workflow JSON missing required node id: {nid}")

    # Inject values
    prompt["244"]["inputs"]["image"] = image_path
    prompt["541"]["inputs"]["num_frames"] = length
    prompt["135"]["inputs"]["positive_prompt"] = user_prompt
    prompt["135"]["inputs"]["negative_prompt"] = negative_prompt

    prompt["220"]["inputs"]["seed"] = seed
    prompt["540"]["inputs"]["seed"] = seed
    prompt["540"]["inputs"]["cfg"] = cfg

    prompt["235"]["inputs"]["value"] = adjusted_width
    prompt["236"]["inputs"]["value"] = adjusted_height

    prompt["498"]["inputs"]["context_overlap"] = context_overlap
    prompt["498"]["inputs"]["context_frames"] = length

    # step settings (optional nodes)
    if "834" in prompt and "829" in prompt:
        prompt["834"]["inputs"]["steps"] = steps
        lowsteps = int(steps * 0.6)
        prompt["829"]["inputs"]["step"] = lowsteps
        logger.info(f"Steps set to: {steps} | LowSteps set to: {lowsteps}")

    # end image (optional)
    if end_image_path_local:
        if "617" not in prompt:
            raise Exception("FLF2V selected but node 617 not found in workflow JSON")
        prompt["617"]["inputs"]["image"] = end_image_path_local

    # LoRA injection (optional)
    if lora_count > 0:
        high_lora_node_id = "279"
        low_lora_node_id = "553"
        if high_lora_node_id in prompt and low_lora_node_id in prompt:
            for i, lora_pair in enumerate(lora_pairs):
                lora_high = (lora_pair or {}).get("high")
                lora_low = (lora_pair or {}).get("low")
                lora_high_weight = (lora_pair or {}).get("high_weight", 1.0)
                lora_low_weight = (lora_pair or {}).get("low_weight", 1.0)

                if lora_high:
                    prompt[high_lora_node_id]["inputs"][f"lora_{i+1}"] = lora_high
                    prompt[high_lora_node_id]["inputs"][f"strength_{i+1}"] = lora_high_weight
                if lora_low:
                    prompt[low_lora_node_id]["inputs"][f"lora_{i+1}"] = lora_low
                    prompt[low_lora_node_id]["inputs"][f"strength_{i+1}"] = lora_low_weight
        else:
            logger.warning("LoRA pairs provided but expected LoRA nodes (279/553) not found in workflow JSON")

    # -----------------------------
    # 5) CONNECT + RUN
    # -----------------------------
    ws_url = f"ws://{server_address}:8188/ws?clientId={client_id}"
    logger.info(f"Connecting to WebSocket: {ws_url}")

    http_url = f"http://{server_address}:8188/"
    logger.info(f"Checking HTTP connection to: {http_url}")

    max_http_attempts = 180
    for http_attempt in range(max_http_attempts):
        try:
            response = urllib.request.urlopen(http_url, timeout=5)
            logger.info(f"HTTP 연결 성공 (시도 {http_attempt+1})")
            break
        except Exception as e:
            logger.warning(f"HTTP 연결 실패 (시도 {http_attempt+1}/{max_http_attempts}): {e}")
            if http_attempt == max_http_attempts - 1:
                raise Exception("ComfyUI 서버에 연결할 수 없습니다. 서버가 실행 중인지 확인하세요.")
            time.sleep(1)

    ws = websocket.WebSocket()
    max_attempts = int(180/5)
    for attempt in range(max_attempts):
        try:
            ws.connect(ws_url)
            logger.info(f"웹소켓 연결 성공 (시도 {attempt+1})")
            break
        except Exception as e:
            logger.warning(f"웹소켓 연결 실패 (시도 {attempt+1}/{max_attempts}): {e}")
            if attempt == max_attempts - 1:
                raise Exception("웹소켓 연결 시간 초과 (3분)")
            time.sleep(5)

    videos = get_videos(ws, prompt)
    ws.close()

    for node_id in videos:
        if videos[node_id]:
            return {"video": videos[node_id][0]}

    return {"error": "비디오를 찾을 수 없습니다."}

runpod.serverless.start({"handler": handler})

