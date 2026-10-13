import json
import subprocess
import sys
import argparse
import tempfile
import os
from urllib.parse import urlparse

# ANSI 颜色代码
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def print_color(text, color):
    print(f"{color}{text}{Colors.ENDC}")

def main():
    parser = argparse.ArgumentParser(description="自动化验证 replay.json (V2 - 解决 Session 丢失问题)")
    parser.add_argument("--replay-file", default="replay.json")
    parser.add_argument("--container", default="replay-test-env")
    parser.add_argument("--target-url", default="http://90.91.181.189:4000")
    parser.add_argument("--dry-run", action="store_true", help="仅打印生成的 Bash 脚本，不执行")
    args = parser.parse_args()

    try:
        parsed_url = urlparse(args.target_url)
        new_host = parsed_url.hostname
        if not new_host: raise ValueError()
    except Exception:
        print_color(f"❌ --target-url 格式错误", Colors.RED)
        sys.exit(1)

    old_host = "172.31.255.1"
    old_url = f"http://{old_host}:3000"

    try:
        with open(args.replay_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print_color(f"❌ 读取 JSON 失败: {e}", Colors.RED)
        sys.exit(1)

    trajectory = data.get("trajectory", [])
    total_steps = len(trajectory)

    print_color(f"🚀 开始生成测试脚本，共 {total_steps} 个步骤...", Colors.BLUE)
    print_color(f"🔗 目标 URL 替换逻辑: {old_host} -> {new_host}\n", Colors.BLUE)

    # 1. 生成 Bash 脚本内容
    # 关键点：所有命令都在同一个 bash 进程中运行，保证 agent-browser session 不丢失
    bash_script_lines = [
        "#!/bin/bash",
        "set +e", # 遇到错误不直接退出，继续执行后续步骤以便观察
        f"export NO_PROXY='{new_host},127.0.0.1,localhost,::1'",
        "cd /root/.openclaw/workspace/tool-modeling/SUB-ALL-BROWSER-01 || exit 1",
        ""
    ]
    
    fail_count = 0

    for i, step in enumerate(trajectory):
        action = step.get("action", "")
        delay = step.get("delay_time", 0.0)

        # 替换 URL
        modified_action = action.replace(old_url, args.target_url)
        modified_action = modified_action.replace(old_host, new_host)
        
        # 清理掉 action 中原本自带的 cd 和 export NO_PROXY，因为我们已经在脚本头部统一设置了
        clean_action = modified_action
        if "cd /root/.openclaw/workspace/tool-modeling/SUB-ALL-BROWSER-01" in clean_action:
             clean_action = clean_action.replace("cd /root/.openclaw/workspace/tool-modeling/SUB-ALL-BROWSER-01 &&", "").strip()
             if clean_action.startswith("&&"): clean_action = clean_action[2:].strip()
        if "export NO_PROXY=" in clean_action:
             parts = clean_action.split("&&")
             clean_parts = [p for p in parts if "export NO_PROXY=" not in p]
             clean_action = "&&".join(clean_parts).strip()
             if clean_action.startswith("&&"): clean_action = clean_action[2:].strip()

        bash_script_lines.append(f"echo '\\n========== [Step {i+1}/{total_steps}] =========='")
        bash_script_lines.append(f"echo 'Executing: {clean_action}'")
        
        # 执行命令
        bash_script_lines.append(f"{clean_action}")
        bash_script_lines.append("STEP_EXIT_CODE=$?")
        bash_script_lines.append("if [ $STEP_EXIT_CODE -ne 0 ]; then")
        bash_script_lines.append(f"  echo '❌ Step {i+1} FAILED with exit code '$STEP_EXIT_CODE")
        bash_script_lines.append("fi")
        
        if delay > 0:
            bash_script_lines.append(f"sleep {delay}")

    bash_script_content = "\n".join(bash_script_lines)

    if args.dry_run:
        print_color("📝 [Dry Run] 生成的 Bash 脚本内容如下:", Colors.YELLOW)
        print(bash_script_content)
        sys.exit(0)

    # 2. 将脚本写入宿主机临时文件
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.sh', encoding='utf-8') as f:
        f.write(bash_script_content)
        host_script_path = f.name

    try:
        # 3. 将脚本拷贝到容器中
        container_script_path = "/tmp/run_replay.sh"
        print_color(f"📥 正在将测试脚本拷贝到容器 {args.container}...", Colors.BLUE)
        cp_result = subprocess.run(["docker", "cp", host_script_path, f"{args.container}:{container_script_path}"], capture_output=True, text=True)
        if cp_result.returncode != 0:
            print_color(f"❌ 拷贝脚本失败: {cp_result.stderr}", Colors.RED)
            sys.exit(1)

        # 4. 在容器中执行脚本 (关键：只执行一次 docker exec，保持 Session 连续)
        print_color(f"🏃 开始在容器内连续执行测试脚本...\n", Colors.BLUE)
        exec_cmd = ["docker", "exec", "-i", args.container, "bash", container_script_path]
        
        process = subprocess.Popen(exec_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding='utf-8')
        
        for line in process.stdout:
            print(line, end='')
            if "FAILED with exit code" in line:
                fail_count += 1
                
        process.wait()
        
        print_color(f"\n🎉 脚本执行完毕！", Colors.BOLD)
        if fail_count > 0:
            print_color(f"❌ 检测到 {fail_count} 个步骤失败 (请查看上方日志中的 'FAILED' 标记)", Colors.RED)
        else:
            print_color(f"✅ 全部 {total_steps} 个步骤执行成功！", Colors.GREEN)
            
    finally:
        # 5. 清理宿主机临时文件
        if os.path.exists(host_script_path):
            os.unlink(host_script_path)

if __name__ == "__main__":
    main()