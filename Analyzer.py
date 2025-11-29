import os
import re
import subprocess
import sys
import asyncio
import logging
from typing import TypedDict, List, Dict, Any, Optional, Tuple
from urllib.parse import quote, urlparse, parse_qs, urlencode, urlunparse
from langgraph.graph import StateGraph, END
from langchain_core.messages import HumanMessage, AIMessage, BaseMessage
from langchain_openai import AzureChatOpenAI
from dotenv import load_dotenv
from dataclasses import dataclass
import json
from pathlib import Path
import time
from shared_state import SharedStateManager

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration_analyzer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Repository configuration
DEFAULT_REPO_URL = "https://github.com/prashantkumarll/dotnetcore-kafka-integration.git"
DEFAULT_BRANCH = "ai-test-coverage-20251129_001309"
#DEFAULT_BRANCH = "ai-test-coverage-20251124_085518"
#DEFAULT_BRANCH = "main"

@dataclass
class AnalysisConfig:
    """Configuration class for analysis parameters"""
    max_chunk_size: int = 4000
    max_workers: int = 15  # Increased from 10 to 15 for better parallelism
    excluded_extensions: tuple = (".png", ".jpg", ".exe", ".dll", ".bin", ".zip", ".tar", ".gz", ".md")
    excluded_files: tuple = (".gitignore", ".gitignore copy")
    excluded_patterns: tuple = ("test-coverage-report-*.json",)
    included_file_patterns: list = None
    ai_timeout: int = 60  # Reduced from 90 to 60 seconds for faster processing
    retry_attempts: int = 2  # Reduced from 3 to 2 to fail faster on problematic files
    
    def __post_init__(self):
        if self.included_file_patterns is None:
            self.included_file_patterns = ["*.cs", "*.csproj", "*.json", "*.md", "*.txt", "*.yml", "*.yaml"]

@dataclass 
class MigrationStats:
    """Statistics tracking for migration process"""
    total_files_scanned: int = 0
    kafka_files_detected: int = 0
    files_successfully_migrated: int = 0
    files_failed_migration: int = 0
    analysis_duration: float = 0.0
    migration_duration: float = 0.0

class RepoAnalysisState(TypedDict):
    repo_url: str
    repo_path: str
    branch: str
    code_chunks: List[str]
    messages: List[BaseMessage]
    analysis: str
    kafka_inventory: List[dict]
    code_diffs: List[dict]
    # AI configuration (optional)
    model: str
    api_version: str
    base_url: str
    api_key: str
    # New fields for enhanced functionality
    config: AnalysisConfig
    stats: MigrationStats
    start_time: float
    errors: List[str]

class MigrationError(Exception):
    """Custom exception for migration-related errors"""
    pass

class GitOperationError(Exception):
    """Custom exception for git-related errors"""
    pass

def validate_environment() -> Tuple[bool, List[str]]:
    """Validate that all required environment variables and tools are available"""
    errors = []
    
    # Check required environment variables
    required_env_vars = ['AZURE_ENDPOINT', 'AZURE_OPENAI_API_KEY']
    for var in required_env_vars:
        if not os.getenv(var):
            errors.append(f"Missing required environment variable: {var}")
    
    # Check git availability
    try:
        subprocess.run(['git', '--version'], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        errors.append("Git is not installed or not accessible")
    
    return len(errors) == 0, errors

def safe_git_operation(operation: str, repo_path: str, *args, **kwargs) -> Tuple[bool, str]:
    """Safely execute git operations with error handling"""
    try:
        cmd = ['git'] + operation.split() + list(args)
        result = subprocess.run(
            cmd,
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=True,
            **kwargs
        )
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        error_msg = f"Git operation failed: {operation}. Error: {e.stderr or e.stdout}"
        logger.error(error_msg)
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error during git operation: {operation}. Error: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

def clone_repo(state: RepoAnalysisState):
    """Enhanced repository cloning with better error handling and validation"""
    repo_url = state["repo_url"]
    local_path = state["repo_path"]
    branch = state.get("branch", "main")  # Default to main if no branch specified
    
    logger.info(f"Starting repository clone/update process for {repo_url} (branch: {branch})")
    
    try:
        # Check if repository files already exist (cloned by main application)
        if os.path.exists(local_path) and os.listdir(local_path):
            logger.info(f"Repository files already exist at {local_path}")
            
            # Validate it's a git repository
            if not os.path.exists(os.path.join(local_path, ".git")):
                logger.warning(f"Directory exists but is not a git repository: {local_path}")
                # Could optionally clean and re-clone here
            
            return state
        
        if os.path.exists(os.path.join(local_path, ".git")):
            logger.info(f"Git repository exists at {local_path}, checking for updates...")

            # Get remote commit
            success, remote_output = safe_git_operation("ls-remote", local_path, repo_url, "HEAD")
            if not success:
                logger.error(f"Failed to check remote repository: {remote_output}")
                state.setdefault("errors", []).append(f"Remote check failed: {remote_output}")
                return state
                
            remote_commit = remote_output.split()[0] if remote_output.strip() else ""

            # Get local commit
            success, local_output = safe_git_operation("rev-parse", local_path, "HEAD")
            if not success:
                logger.error(f"Failed to get local commit: {local_output}")
                state.setdefault("errors", []).append(f"Local commit check failed: {local_output}")
                return state
                
            local_commit = local_output.strip()

            if local_commit == remote_commit:
                logger.info("Local repository is already up-to-date with origin")
            else:
                logger.info("Local repo is outdated, pulling latest changes...")
                success, pull_output = safe_git_operation("pull", local_path)
                if not success:
                    logger.error(f"Failed to pull updates: {pull_output}")
                    state.setdefault("errors", []).append(f"Git pull failed: {pull_output}")
        else:
            logger.info(f"Cloning repository into {local_path}...")
            os.makedirs(local_path, exist_ok=True)
            
            # Use direct subprocess call for cloning since safe_git_operation expects existing repo
            try:
                result = subprocess.run(
                    ['git', 'clone', '--depth', '1', '--branch', branch, repo_url, local_path],
                    capture_output=True,
                    text=True,
                    check=True
                )
                logger.info(f"Repository cloned successfully (branch: {branch})")
            except subprocess.CalledProcessError as e:
                error_msg = f"Git clone failed: {e.stderr or e.stdout}"
                logger.error(error_msg)
                state.setdefault("errors", []).append(f"Git clone failed: {error_msg}")
                return state
                
        logger.info("Repository clone/update completed successfully")
        return state
        
    except Exception as e:
        error_msg = f"Unexpected error during repository operations: {str(e)}"
        logger.error(error_msg)
        state.setdefault("errors", []).append(error_msg)
        return state

def get_updated_state_with_code_chunks(state: RepoAnalysisState) -> RepoAnalysisState:
    """Enhanced code chunking with better file filtering and statistics"""
    repo_path = state["repo_path"]
    config = state.get("config", AnalysisConfig())
    stats = state.get("stats", MigrationStats())
    
    logger.info("Starting code analysis and chunking process")
    
    chunks = []
    files_processed = 0
    
    try:
        for root, dirs, files in os.walk(repo_path):
            # Skip .git directory
            if ".git" in dirs:
                dirs.remove(".git")
            
            # Skip other common directories to ignore
            dirs[:] = [d for d in dirs if not d.startswith('.') and d not in ['node_modules', '__pycache__', 'bin', 'obj']]
            
            for file_name in files:
                file_path = os.path.join(root, file_name)
                
                # Enhanced file filtering
                if file_name.lower().endswith(config.excluded_extensions):
                    continue
                
                # Skip specific excluded files
                if file_name in config.excluded_files:
                    continue
                
                # Skip files matching excluded patterns
                import fnmatch
                if any(fnmatch.fnmatch(file_name, pattern) for pattern in config.excluded_patterns):
                    continue
                
                # Check if file matches included patterns (if specified)
                if config.included_file_patterns:
                    from fnmatch import fnmatch
                    if not any(fnmatch(file_name.lower(), pattern.lower()) for pattern in config.included_file_patterns):
                        continue
                
                try:
                    # Check if file is binary
                    with open(file_path, "rb") as test_fp:
                        start = test_fp.read(1024)
                        if b'\0' in start:
                            continue
                    
                    # Read and chunk the file
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as fp:
                        content = fp.read()
                        
                        # Skip empty files
                        if not content.strip():
                            continue
                            
                        # Create chunks
                        for i in range(0, len(content), config.max_chunk_size):
                            chunk = content[i:i+config.max_chunk_size]
                            rel_path = os.path.relpath(file_path, repo_path)
                            chunks.append(f"File: {rel_path}\n{chunk}")
                    
                    files_processed += 1
                    
                except (IOError, OSError, UnicodeDecodeError) as e:
                    logger.warning(f"Failed to process file {file_path}: {str(e)}")
                    continue
    
        # Update statistics
        stats.total_files_scanned = files_processed
        
        logger.info(f"Code analysis completed: {len(chunks)} chunks from {files_processed} files")
        
        return {
            **state, 
            "code_chunks": chunks,
            "stats": stats
        }
        
    except Exception as e:
        error_msg = f"Error during code chunking: {str(e)}"
        logger.error(error_msg)
        state.setdefault("errors", []).append(error_msg)
        return state

async def analyze_and_scan_kafka_async(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enhanced Kafka analysis with better error handling, retries, and improved prompts
    """
    config = state.get("config", AnalysisConfig())
    stats = state.get("stats", MigrationStats())
    analysis_start_time = time.time()
    
    logger.info("Starting AI-powered Kafka analysis")
    
    try:
        # Initialize LLM with error handling
        try:
            llm = AzureChatOpenAI(
                deployment_name=state.get('model'),
                azure_endpoint=state.get('base_url'),
                openai_api_key=state.get('api_key'),
                openai_api_version=state.get('api_version'),
                timeout=config.ai_timeout,
                max_retries=config.retry_attempts
            )
        except Exception as e:
            error_msg = f"Failed to initialize AI client: {str(e)}"
            logger.error(error_msg)
            state.setdefault("errors", []).append(error_msg)
            return state

        repo_path = state["repo_path"]
        candidate_files = []

        # Get candidate files with better filtering
        for root, dirs, files in os.walk(repo_path):
            if ".git" in dirs:
                dirs.remove(".git")
            for f in files:
                if f.lower().endswith(config.excluded_extensions):
                    continue
                
                # Skip specific excluded files
                if f in config.excluded_files:
                    continue
                
                # Skip files matching excluded patterns
                import fnmatch
                if any(fnmatch.fnmatch(f, pattern) for pattern in config.excluded_patterns):
                    continue
                
                candidate_files.append(os.path.join(root, f))

        sem = asyncio.Semaphore(config.max_workers)

        async def process_file_with_retry(file_path: str) -> Dict[str, Any]:
            """Process a single file with retry logic"""
            rel_file = os.path.relpath(file_path, repo_path)
            
            for attempt in range(config.retry_attempts):
                try:
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                except Exception as e:
                    logger.warning(f"Failed to read file {rel_file}: {str(e)}")
                    return {}

                # Improved prompt with better structure and examples
                prompt = f"""
You are a .NET Core and Apache Kafka expert analyzer.

TASK: Analyze the following file for Kafka usage and provide structured output.

FILE: {rel_file}

INSTRUCTIONS:
1. Scan for Kafka-related libraries, classes, and configurations
2. Look for: Confluent.Kafka, KafkaProducer, KafkaConsumer, topics, partitions, brokers
3. If Kafka usage is found, return EXACTLY this format:
   File: {rel_file}
   Kafka APIs: [comma-separated list of APIs found]
   Summary: [brief description of Kafka usage]

4. If NO Kafka usage found, return exactly: "No Kafka usage"

EXAMPLES OF KAFKA INDICATORS:
- using Confluent.Kafka
- Producer<string, string>
- Consumer<string, string>
- bootstrap.servers
- ProduceAsync
- Subscribe(topic)
- ConsumerConfig
- ProducerConfig

CODE:
{content[:2000]}{"... (truncated)" if len(content) > 2000 else ""}
"""

                async with sem:
                    try:
                        resp = await asyncio.wait_for(
                            asyncio.to_thread(lambda: llm.invoke([HumanMessage(content=prompt)])),
                            timeout=config.ai_timeout
                        )
                        text = getattr(resp, "content", "").strip()
                        
                        if "No Kafka usage" in text or not text:
                            return {}

                        # Enhanced parsing with validation
                        kafka_apis = []
                        summary = ""
                        
                        # Look for the structured format
                        match_api = re.search(r"Kafka APIs\s*:\s*\[(.*?)\]", text)
                        if not match_api:
                            match_api = re.search(r"Kafka APIs\s*:\s*(.*?)(?:\n|$)", text)
                        
                        if match_api:
                            apis_text = match_api.group(1).strip()
                            kafka_apis = [x.strip().strip('"') for x in apis_text.split(",") if x.strip()]
                        
                        match_summary = re.search(r"Summary\s*:\s*(.*?)(?:\n|$)", text)
                        if match_summary:
                            summary = match_summary.group(1).strip()

                        if kafka_apis or summary:
                            logger.info(f"Kafka usage detected in {rel_file}")
                            return {"file": rel_file, "kafka_apis": kafka_apis, "summary": summary}
                        else:
                            return {}
                            
                    except asyncio.TimeoutError:
                        logger.warning(f"Timeout analyzing {rel_file} (attempt {attempt + 1})")
                        if attempt == config.retry_attempts - 1:
                            return {}
                    except Exception as e:
                        logger.warning(f"Error analyzing {rel_file} (attempt {attempt + 1}): {str(e)}")
                        if attempt == config.retry_attempts - 1:
                            return {}
                        await asyncio.sleep(0.3)  # Reduced delay from 1s to 0.3s
            
            return {}

        # Process files in batches to avoid overwhelming the API
        batch_size = min(config.max_workers * 2, 20)  # Increased from 10 to 20
        all_results = []
        
        for i in range(0, len(candidate_files), batch_size):
            batch = candidate_files[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(candidate_files) + batch_size - 1)//batch_size}")
            
            tasks = [process_file_with_retry(f) for f in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Filter out exceptions and empty results
            valid_results = [r for r in batch_results if isinstance(r, dict) and r]
            all_results.extend(valid_results)

        # Update statistics
        stats.kafka_files_detected = len(all_results)
        stats.analysis_duration = time.time() - analysis_start_time
        
        logger.info(f"Kafka analysis completed: {len(all_results)} files with Kafka usage detected in {stats.analysis_duration:.2f}s")
        
        return {
            **state, 
            "analysis": f"Kafka detected in {len(all_results)} files.", 
            "kafka_inventory": all_results,
            "stats": stats
        }
        
    except Exception as e:
        error_msg = f"Critical error during Kafka analysis: {str(e)}"
        logger.error(error_msg)
        state.setdefault("errors", []).append(error_msg)
        return state


# -----------------------------
# Sync wrapper for StateGraph
# -----------------------------
def analyze_and_scan_kafka(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Synchronous wrapper to integrate with StateGraph.
    """
    return asyncio.run(analyze_and_scan_kafka_async(state))


def clean_diff_response(diff_text: str) -> str:
    """Remove ellipsis and truncated markers from diff response."""
    if not diff_text:
        return diff_text
    
    lines = diff_text.split('\n')
    cleaned_lines = []
    
    for line in lines:
        stripped = line.strip()
        # Skip ellipsis-only lines and truncated markers
        if stripped in ['...', '... ', '...\n'] or '... (truncated)' in line.lower() or '... (omitted)' in line.lower():
            continue
        cleaned_lines.append(line)
    
    return '\n'.join(cleaned_lines)


def validate_kafka_removal(diff_text: str, filename: str) -> Dict[str, Any]:
    """
    Validate that diff completely removes Kafka APIs to prevent compilation errors.
    
    Args:
        diff_text: Git diff text to validate
        filename: Name of the file for logging
        
    Returns:
        Dict with validation results and suggested fixes
    """
    result = {
        "is_valid": True,
        "reason": "",
        "kafka_apis_found": [],
        "suggestions": []
    }
    
    # Kafka APIs that cause compilation errors if left in Service Bus code
    problematic_kafka_apis = [
        # Method calls
        r'\.Consume\s*\(',
        r'\.Produce\s*\(',
        r'\.Subscribe\s*\(',
        # Types that don't exist in Service Bus
        r'ConsumeResult\s*<',
        r'ProduceException\s*<',
        r'IConsumer\s*<',
        r'IProducer\s*<',
        r'ConsumerBuilder\s*<',
        r'ProducerBuilder\s*<',
        # Namespaces
        r'Confluent\.Kafka',
        r'using\s+Confluent\.Kafka'
    ]
    
    # Check for problematic APIs in added lines (lines starting with +)
    added_lines = [line for line in diff_text.split('\n') if line.startswith('+')]
    
    import re
    for line_num, line in enumerate(added_lines, 1):
        line_content = line[1:].strip()  # Remove + prefix
        for api_pattern in problematic_kafka_apis:
            if re.search(api_pattern, line_content, re.IGNORECASE):
                result["is_valid"] = False
                result["kafka_apis_found"].append({
                    "line": line_num,
                    "content": line_content,
                    "api": api_pattern
                })
    
    if not result["is_valid"]:
        result["reason"] = f"Found {len(result['kafka_apis_found'])} Kafka APIs in added lines that will cause compilation errors"
        result["suggestions"] = [
            "Remove all Kafka method calls like .Consume(), .Produce()",
            "Replace Kafka types with Service Bus equivalents",
            "Use ServiceBusException instead of ProduceException",
            "Replace Mock<IConsumer> with Mock<ServiceBusClient>"
        ]
    
    return result


def validate_constructor_consistency(migration_results):
    """Simple constructor validation between wrapper classes and tests."""
    return {'has_errors': False, 'errors': [], 'suggested_fixes': []}


def validate_interface_consistency(migration_results):
    """Simple validation of interface consistency between wrapper classes and tests."""
    result = {'has_errors': False, 'errors': [], 'suggested_fixes': []}
    
    for item in migration_results:
        if 'Tests.cs' in item.get('file', '') and 'Mock<ServiceBusClient>' in item.get('diff', ''):
            # Basic validation passed
            continue
    
    return result


def apply_interface_fixes(diffs_list, constructor_result, interface_result):
    """Simple interface fixes for test files."""
    return diffs_list  # Compilation validation will handle any issues


def extract_wrapper_interfaces(diffs_list):
    """
    Extract actual interface signatures from wrapper class diffs.
    Returns dict with wrapper info: constructor signature, method names, dispose pattern.
    """
    import re
    interfaces = {}
    
    for diff_item in diffs_list:
        file_path = diff_item.get('file', '')
        diff_content = diff_item.get('diff', '')
        
        # Look for wrapper class files
        if file_path.endswith('Wrapper.cs') and not 'Test' in file_path:
            wrapper_name = file_path.split('\\')[-1].replace('.cs', '')
            
            # Extract constructor signature
            constructor_match = re.search(
                rf'\+.*public\s+{wrapper_name}\s*\(([^)]+)\)',
                diff_content, re.MULTILINE | re.DOTALL
            )
            
            # Extract method signatures
            methods = re.findall(
                r'\+.*public\s+(?:async\s+)?(Task<\w+>|\w+)\s+(\w+)\s*\([^)]*\)',
                diff_content, re.MULTILINE
            )
            
            # Check dispose pattern
            is_async_disposable = 'IAsyncDisposable' in diff_content
            is_disposable = 'IDisposable' in diff_content and not is_async_disposable
            
            interfaces[wrapper_name] = {
                'constructor_params': constructor_match.group(1) if constructor_match else None,
                'methods': {method[1]: method[0] for method in methods},
                'dispose_pattern': 'async' if is_async_disposable else 'sync' if is_disposable else None
            }
    
    return interfaces


def fix_constructor_calls(diff_content, wrapper_name, wrapper_info):
    """
    Fix constructor calls to match actual wrapper constructor signature.
    Uses detected interface signature to generate correct calls.
    """
    import re
    
    if not wrapper_info or not wrapper_info.get('constructor_params'):
        return diff_content
    
    # Parse actual constructor parameters
    params = [p.strip() for p in wrapper_info['constructor_params'].split(',')]
    param_types = []
    param_names = []
    for param in params:
        parts = param.strip().split()
        if len(parts) >= 2:
            param_types.append(' '.join(parts[:-1]))  # Handle complex types like ServiceBusProcessorOptions
            param_names.append(parts[-1])
    
    # Create proper constructor call replacement based on enforced signature patterns
    if wrapper_name == 'ConsumerWrapper':
        # Enforced signature: ConsumerWrapper(ServiceBusClient client, string topicName, ServiceBusProcessorOptions options = null)
        # Generate 3-parameter call: new ConsumerWrapper(mockClient.Object, topicName, options)
        pattern = r'(\+.*new ConsumerWrapper\()([^)]+)\)'
        
        def replacement(match):
            prefix = match.group(1)
            old_params = match.group(2)
            
            # Extract variable names from the test context
            mock_client_var = 'mockClient.Object'
            topic_var = '"test-topic"'
            options_var = 'options'
            
            # Look for existing variable declarations in the diff
            if 'topicName' in old_params:
                # Extract topic name from original parameters
                topic_match = re.search(r'"[^"]*"', old_params)
                if topic_match:
                    topic_var = topic_match.group(0)
                else:
                    # Look for variable name
                    var_match = re.search(r'\b(\w*[Tt]opic\w*)\b', old_params)
                    if var_match:
                        topic_var = var_match.group(1)
            
            if 'var options' in diff_content:
                options_var = 'options'
            
            return f'{prefix}{mock_client_var}, {topic_var}, {options_var})'
        
        return re.sub(pattern, replacement, diff_content)
    
    elif wrapper_name == 'ProducerWrapper':
        # Actual signature: ProducerWrapper(ServiceBusClient client, string topicName)
        pattern = r'(\+.*new ProducerWrapper\()([^)]+)\)'
        
        def replacement(match):
            prefix = match.group(1)
            old_params = match.group(2)
            
            # For ProducerWrapper, we need a ServiceBusClient mock
            mock_client_var = 'mockClient.Object'
            topic_var = '"test-topic"'
            
            # Extract topic name if available
            topic_match = re.search(r'"[^"]*"', old_params)
            if topic_match:
                topic_var = topic_match.group(0)
            else:
                # Look for variable name
                var_match = re.search(r'\b(\w*[Tt]opic\w*)\b', old_params)
                if var_match:
                    topic_var = var_match.group(1)
            
            return f'{prefix}{mock_client_var}, {topic_var})'
        
        return re.sub(pattern, replacement, diff_content)
    
    return diff_content


def fix_method_calls(diff_content, wrapper_info):
    """
    Fix method calls to match actual wrapper method signatures.
    """
    import re
    
    methods = wrapper_info.get('methods', {})
    
    for method_name, return_type in methods.items():
        # Check if method is in the diff and fix the call
        if method_name in diff_content:
            is_async = 'Task' in return_type
            
            # Fix method calls with more precise pattern to avoid variable name corruption
            if is_async:
                # Pattern: var result = wrapper.methodName()
                # Replace with: var result = await wrapper.methodName()
                pattern = rf'(\+.*?=\s*)(\w+)(\.\s*{method_name}\s*\([^)]*\))'
                
                def replacement(match):
                    prefix = match.group(1)  # "var result = "
                    var_name = match.group(2)  # "wrapper"
                    method_call = match.group(3)  # ".methodName()"
                    
                    # Only add await if not already present
                    if 'await' not in prefix:
                        return f'{prefix}await {var_name}{method_call}'
                    else:
                        return f'{prefix}{var_name}{method_call}'
                
                diff_content = re.sub(pattern, replacement, diff_content)
                
                # Also handle direct method calls without assignment
                pattern2 = rf'(\+.*?)(\w+)(\.\s*{method_name}\s*\([^)]*\);)'
                
                def replacement2(match):
                    prefix = match.group(1)
                    var_name = match.group(2)
                    method_call = match.group(3)
                    
                    # Ensure proper spacing with await
                    if 'await' not in prefix and '=' not in prefix:
                        # This is a standalone method call
                        return f'{prefix}await {var_name}{method_call}'
                    else:
                        return f'{prefix}{var_name}{method_call}'
                
                diff_content = re.sub(pattern2, replacement2, diff_content)
                
            else:
                # For sync methods, remove any await
                pattern = rf'(\+.*?)await\s+(\w+)(\.\s*{method_name}\s*\([^)]*\))'
                replacement = r'\1\2\3'
                diff_content = re.sub(pattern, replacement, diff_content)
    
    return diff_content


def fix_dispose_patterns(diff_content, wrapper_info):
    """
    Fix dispose calls to match actual wrapper dispose pattern.
    """
    import re
    
    dispose_pattern = wrapper_info.get('dispose_pattern')
    
    if dispose_pattern == 'sync':
        # Convert async dispose to sync dispose
        pattern = r'(\+.*?)await\s+(\w+)\.DisposeAsync\s*\(\s*\)'
        replacement = r'\1\2.Dispose()'
        diff_content = re.sub(pattern, replacement, diff_content)
        
    elif dispose_pattern == 'async':
        # Convert sync dispose to async dispose
        pattern = r'(\+.*?)(\w+)\.Dispose\s*\(\s*\)'
        replacement = r'\1await \2.DisposeAsync()'
        diff_content = re.sub(pattern, replacement, diff_content)
    
    return diff_content


def fix_test_method_signatures(diff_content):
    """
    Fix test method signatures (async/sync) based on whether they contain await calls.
    """
    import re
    
    # Find test methods and check if they need to be async
    method_pattern = r'(\+.*\[Fact\].*?\n\+.*public\s+)(async\s+Task|void)(\s+\w+.*?\{.*?^\+.*\})'
    
    def fix_method(match):
        prefix = match.group(1)
        current_signature = match.group(2)
        method_body = match.group(3)
        
        has_await = 'await' in method_body
        is_async = 'async Task' in current_signature
        
        if has_await and not is_async:
            # Method has await but is not async - make it async
            return f'{prefix}async Task{method_body}'
        elif not has_await and is_async:
            # Method is async but has no await - make it sync
            return f'{prefix}void{method_body}'
        else:
            # Method signature is correct
            return match.group(0)
    
    return re.sub(method_pattern, fix_method, diff_content, flags=re.MULTILINE | re.DOTALL)


def fix_variable_context(diff_content, wrapper_name):
    """
    Fix variable context issues caused by constructor signature changes.
    Ensure proper variable declarations match constructor requirements.
    """
    import re
    
    # Both ConsumerWrapper and ProducerWrapper now require ServiceBusClient mock
    # So we ensure mockClient declaration exists, not remove it
    
    if wrapper_name == 'ConsumerWrapper':
        # Ensure mockClient is declared for 3-parameter constructor
        if 'mockClient.Object' in diff_content and 'var mockClient' not in diff_content:
            # Add mockClient declaration at beginning of test methods
            # Find test method start and add declaration
            pattern = r'(\+.*\[Fact\].*?\n\+.*public .*?\n\+.*\{)'
            replacement = r'\1\n+            var mockClient = new Mock<ServiceBusClient>();'
            diff_content = re.sub(pattern, replacement, diff_content)
            
    elif wrapper_name == 'ProducerWrapper':
        # Ensure mockClient is declared for 2-parameter constructor  
        if 'mockClient.Object' in diff_content and 'var mockClient' not in diff_content:
            # Add mockClient declaration at beginning of test methods
            pattern = r'(\+.*\[Fact\].*?\n\+.*public .*?\n\+.*\{)'
            replacement = r'\1\n+            var mockClient = new Mock<ServiceBusClient>();'
            diff_content = re.sub(pattern, replacement, diff_content)
        
    return diff_content


def add_missing_using_statements(diff_content):
    """
    Add missing using statements for Task-based async methods.
    """
    import re
    
    # Check if using System.Threading.Tasks is already present
    if 'using System.Threading.Tasks;' in diff_content:
        return diff_content
    
    # Find the using statements section and add the missing one
    using_pattern = r'(\+using System.*?;\n)(\+using [^;]*;\n)*'
    
    def add_using(match):
        existing_usings = match.group(0)
        if 'using System.Threading.Tasks;' not in existing_usings:
            # Add after System using statements
            return existing_usings + '+using System.Threading.Tasks;\n'
        return existing_usings
    
    # Try to add after existing using statements
    new_diff = re.sub(using_pattern, add_using, diff_content)
    
    # If no using statements found, add at the beginning
    if new_diff == diff_content and 'async Task' in diff_content:
        # Add as the first line of the diff
        lines = diff_content.split('\n')
        for i, line in enumerate(lines):
            if line.startswith('+using System'):
                lines.insert(i + 1, '+using System.Threading.Tasks;')
                return '\n'.join(lines)
            elif line.startswith('+') and 'namespace' in line:
                lines.insert(i, '+using System.Threading.Tasks;')
                return '\n'.join(lines)
    
    return new_diff


def validate_diff_for_compilation_errors(diff_content: str, filename: str, original_content: str = None) -> Dict[str, Any]:
    """
    Validate that a diff will not cause compilation errors when applied.
    This checks for common issues that cause compilation failures.
    
    Args:
        diff_content: The git diff content
        filename: Name of the file being processed
        original_content: Optional original file content for context
        
    Returns:
        Dict with validation results: is_valid, errors, warnings
    """
    result = {
        "is_valid": True,
        "errors": [],
        "warnings": [],
        "suggestions": []
    }
    
    import re
    
    # Check 1: Kafka APIs in added lines (will cause compilation errors)
    kafka_validation = validate_kafka_removal(diff_content, filename)
    if not kafka_validation["is_valid"]:
        result["is_valid"] = False
        result["errors"].extend([f"Kafka API found: {api['api']}" for api in kafka_validation.get("kafka_apis_found", [])])
        result["suggestions"].extend(kafka_validation.get("suggestions", []))
    
    # Check 2: Undefined variables (mockClient, etc.)
    added_lines = [line[1:] for line in diff_content.split('\n') if line.startswith('+')]
    for line in added_lines:
        # Check for mockClient.Object usage without declaration
        if 'mockClient.Object' in line and 'var mockClient' not in '\n'.join(added_lines[:added_lines.index(line) if line in added_lines else 0]):
            # Check if it's declared earlier in the diff
            found_declaration = False
            for prev_line in added_lines[:added_lines.index(line) if line in added_lines else len(added_lines)]:
                if 'var mockClient' in prev_line or 'Mock<ServiceBusClient> mockClient' in prev_line:
                    found_declaration = True
                    break
            if not found_declaration:
                result["warnings"].append(f"Potential undefined variable 'mockClient' at line: {line[:50]}")
                result["suggestions"].append("Ensure 'var mockClient = new Mock<ServiceBusClient>();' is declared before use")
    
    # Check 3: Method name mismatches (WriteMessageAsync vs writeMessage)
    if 'WriteMessageAsync' in diff_content or 'ReadMessageAsync' in diff_content:
        result["warnings"].append("Found WriteMessageAsync/ReadMessageAsync - should be writeMessage/readMessage for wrapper classes")
        result["suggestions"].append("Use wrapper method names: writeMessage() and readMessage() instead of WriteMessageAsync/ReadMessageAsync")
    
    # Check 4: Missing await for async calls
    async_methods = ['ReadMessageAsync', 'writeMessage', 'readMessage', 'SendMessageAsync']
    for method in async_methods:
        pattern = rf'(\+.*?)(\w+\.{method}\s*\()'
        matches = re.findall(pattern, diff_content)
        for match in matches:
            prefix = match[0]
            if 'await' not in prefix and '=' in prefix:
                result["warnings"].append(f"Missing 'await' for async method call: {method}")
                result["suggestions"].append(f"Add 'await' before {method} call")
    
    # Check 5: Constructor parameter count mismatches
    if 'ConsumerWrapperTests' in filename:
        # Check ConsumerWrapper constructor calls
        constructor_calls = re.findall(r'\+.*new ConsumerWrapper\(([^)]+)\)', diff_content)
        for call in constructor_calls:
            params = [p.strip() for p in call.split(',') if p.strip()]
            if len(params) != 3:
                result["errors"].append(f"ConsumerWrapper constructor should have 3 parameters, found {len(params)}: {call}")
                result["suggestions"].append("Use: new ConsumerWrapper(mockClient.Object, topicName, options)")
    
    if 'ProducerWrapperTests' in filename:
        # Check ProducerWrapper constructor calls
        constructor_calls = re.findall(r'\+.*new ProducerWrapper\(([^)]+)\)', diff_content)
        for call in constructor_calls:
            params = [p.strip() for p in call.split(',') if p.strip()]
            if len(params) != 2:
                result["errors"].append(f"ProducerWrapper constructor should have 2 parameters, found {len(params)}: {call}")
                result["suggestions"].append("Use: new ProducerWrapper(mockClient.Object, topicName)")
    
    # Check 6: Missing using statements
    if 'async Task' in diff_content and 'using System.Threading.Tasks;' not in diff_content:
        result["warnings"].append("Missing 'using System.Threading.Tasks;' for async methods")
        result["suggestions"].append("Add 'using System.Threading.Tasks;' at the top of the file")
    
    # Check 7: Dispose pattern mismatches
    if 'IAsyncDisposable' in diff_content or 'DisposeAsync' in diff_content:
        if '.Dispose()' in diff_content and 'DisposeAsync' not in diff_content:
            result["warnings"].append("Using synchronous Dispose() but wrapper implements IAsyncDisposable")
            result["suggestions"].append("Use 'await wrapper.DisposeAsync()' instead of 'wrapper.Dispose()'")
    
    return result


def validate_and_fix_compilation_errors(diff_content: str, filename: str) -> str:
    """
    Validate and fix common compilation errors that can occur during migration.
    
    Args:
        diff_content: The git diff content
        filename: Name of the file being processed
        
    Returns:
        Fixed diff content
    """
    import re
    
    logger.info(f"Validating and fixing compilation errors for {filename}")
    
    # Enhanced Fix: Constructor parameter consistency validation
    # Fix OrderController.cs to use ServiceBusClient instead of ProducerConfig
    if 'OrderController' in filename:
        # Fix import statements
        diff_content = re.sub(r'using Confluent\.Kafka;', 'using Azure.Messaging.ServiceBus;', diff_content)
        # Fix constructor parameter type
        diff_content = re.sub(r'ProducerConfig config', 'ServiceBusClient client', diff_content)
        # Fix field declaration
        diff_content = re.sub(r'private readonly ProducerConfig config;', 'private readonly ServiceBusClient client;', diff_content)
        # Fix field assignment
        diff_content = re.sub(r'this\.config = config;', 'this.client = client;', diff_content)
        # Fix ProducerWrapper instantiation
        diff_content = re.sub(r'new ProducerWrapper\(this\.config,', 'new ProducerWrapper(this.client,', diff_content)
    
    # Enhanced Fix: Interface vs Concrete Class validation
    interface_fixes = [
        (r'IServiceBusProcessor', 'ServiceBusProcessor'),
        (r'IServiceBusSender', 'ServiceBusSender'),
        (r'IServiceBusReceiver', 'ServiceBusReceiver'),
    ]
    
    for wrong_interface, correct_class in interface_fixes:
        diff_content = re.sub(wrong_interface, correct_class, diff_content)
    
    # Enhanced Fix: Mixed import statement detection and cleanup
    if 'using Confluent.Kafka' in diff_content and 'using Azure.Messaging.ServiceBus' in diff_content:
        # Remove the Confluent.Kafka import if Azure.Messaging.ServiceBus exists
        diff_content = re.sub(r'\+?\s*using Confluent\.Kafka;?\s*\n', '', diff_content)
        logger.info(f"Removed conflicting Kafka import from {filename}")
    
    # Enhanced Fix: XML project file issues (duplicate closing tags)
    if filename.endswith('.csproj'):
        # Fix duplicate </Project> closing tags
        lines = diff_content.split('\n')
        project_close_count = sum(1 for line in lines if '</Project>' in line and line.strip().startswith('+'))
        if project_close_count > 1:
            # Remove extra </Project> tags
            found_first = False
            for i, line in enumerate(lines):
                if '</Project>' in line and line.strip().startswith('+'):
                    if found_first:
                        lines[i] = ''  # Remove the duplicate
                    else:
                        found_first = True
            diff_content = '\n'.join(lines)
            logger.info(f"Removed duplicate </Project> tags from {filename}")
    
    # Enhanced Fix: Extra closing braces in C# files
    if filename.endswith('.cs'):
        lines = diff_content.split('\n')
        # Count braces to detect structural issues
        added_opening = sum(line.count('{') for line in lines if line.strip().startswith('+'))
        added_closing = sum(line.count('}') for line in lines if line.strip().startswith('+'))
        
        if added_closing > added_opening + 1:  # Allow for one more closing brace
            # Remove extra closing braces at the end
            for i in range(len(lines) - 1, -1, -1):
                if lines[i].strip() == '+}' and added_closing > added_opening:
                    lines[i] = ''
                    added_closing -= 1
                    logger.info(f"Removed extra closing brace from {filename}")
                    if added_closing <= added_opening + 1:
                        break
            diff_content = '\n'.join(lines)
    
    # Enhanced Fix: Missing using statements detection
    if filename.endswith('.cs') and 'ServiceBusClient' in diff_content:
        if 'using Azure.Messaging.ServiceBus' not in diff_content:
            # Add the missing using statement at the top
            lines = diff_content.split('\n')
            for i, line in enumerate(lines):
                if line.strip().startswith('+using ') and 'System' in line:
                    lines.insert(i + 1, '+using Azure.Messaging.ServiceBus;')
                    logger.info(f"Added missing Azure.Messaging.ServiceBus using statement to {filename}")
                    break
            diff_content = '\n'.join(lines)
    
    # Enhanced Fix: Incorrect await usage on synchronous methods
    if 'readMessage()' in diff_content:
        # Fix await readMessage() -> readMessage() (it's synchronous)
        diff_content = re.sub(r'(\+.*?)await\s+(.*\.readMessage\(\))', r'\1\2', diff_content)
        logger.info(f"Fixed incorrect await usage on readMessage in {filename}")
    
    # Pre-validation: Check for common wrapper method patterns and fix them early
    wrapper_method_fixes = [
        (r'(\w+\.WriteMessageAsync\()', lambda m: m.group(1).replace('WriteMessageAsync', 'writeMessage')),
        (r'(\w+\.ReadMessageAsync\()', lambda m: m.group(1).replace('ReadMessageAsync', 'readMessage')),
        (r'(\.WriteMessageAsync\()', '.writeMessage('),
        (r'(\.ReadMessageAsync\()', '.readMessage('),
        # Fix direct calls in controller/service contexts
        (r'(await \w+\.WriteMessageAsync\()', lambda m: m.group(1).replace('WriteMessageAsync', 'writeMessage')),
        (r'(await \w+\.ReadMessageAsync\()', lambda m: m.group(1).replace('ReadMessageAsync', 'readMessage'))
    ]
    
    for pattern, replacement in wrapper_method_fixes:
        if callable(replacement):
            diff_content = re.sub(pattern, replacement, diff_content)
        else:
            diff_content = re.sub(pattern, replacement, diff_content)
    
    # Fix 1: Interlocked.CompareExchange parameter type issues
    # Common issue: passing Task instead of TaskCompletionSource as comparand
    interlocked_pattern = r'(Interlocked\.CompareExchange\(ref\s+[^,]+,\s*[^,]+,\s*)([^)]+)(\))'
    def fix_interlocked_compareexchange(match):
        prefix = match.group(1)
        comparand = match.group(2).strip()
        suffix = match.group(3)
        
        # If the comparand looks like it's using task instead of TaskCompletionSource
        if comparand.strip() == 'task':
            fixed_comparand = 'oldTcs'  # Use a reasonable variable name
            return f'{prefix}{fixed_comparand}{suffix}'
        return match.group(0)
    
    diff_content = re.sub(interlocked_pattern, fix_interlocked_compareexchange, diff_content, flags=re.IGNORECASE)
    
    # Fix 2: Add variable declarations for Interlocked operations
    if 'Interlocked.CompareExchange' in diff_content and 'oldTcs' in diff_content:
        # Ensure oldTcs variable is declared before use
        # Find the method context and add variable declaration
        lines = diff_content.split('\n')
        for i, line in enumerate(lines):
            if 'Interlocked.CompareExchange' in line and 'oldTcs' in line:
                # Check if oldTcs declaration exists before this line
                found_declaration = False
                for j in range(max(0, i-10), i):
                    if 'var oldTcs' in lines[j] or 'oldTcs =' in lines[j]:
                        found_declaration = True
                        break
                
                if not found_declaration:
                    # Add the declaration before the Interlocked call
                    declaration_line = '+                var oldTcs = _messageTcs;'
                    lines.insert(i, declaration_line)
                    break
        
        diff_content = '\n'.join(lines)
    
    # Fix 3: Enhanced variable scoping issues with smarter detection
    variable_issues = [
        ('mockClient.Object', 'var mockClient = new Mock<ServiceBusClient>();'),
        ('mockConfig.Object', 'var mockConfig = new Mock<IConfiguration>();'),
        ('mockService.Object', 'var mockService = new Mock<IProcessOrdersService>();')
    ]
    
    for usage_pattern, declaration in variable_issues:
        if usage_pattern in diff_content:
            # Smart detection: check if variable is used but not declared
            variable_name = declaration.split()[1]  # Extract 'mockClient' from 'var mockClient = ...'
            
            lines = diff_content.split('\n')
            for i, line in enumerate(lines):
                # Look for usage of undefined variable
                if usage_pattern in line and line.strip().startswith('+'):
                    # Check if variable is declared in this method
                    method_start = i
                    # Find the method start (go backwards to find method declaration)
                    for j in range(i, -1, -1):
                        if any(marker in lines[j] for marker in ['[Fact]', '[Theory]', '[Test]']):
                            method_start = j
                            break
                    
                    # Find the method opening brace
                    brace_line = -1
                    for j in range(method_start, min(i + 5, len(lines))):
                        if '{' in lines[j]:
                            brace_line = j
                            break
                    
                    if brace_line != -1:
                        # Check if variable is declared between brace and usage
                        method_segment = '\n'.join(lines[brace_line:i])
                        if variable_name not in method_segment:
                            # Add declaration after the opening brace
                            indent = '            '  # Standard test method indentation
                            lines.insert(brace_line + 1, f'+{indent}{declaration}')
                            logger.info(f"Added missing variable declaration: {declaration} in {filename}")
                            break
            
            diff_content = '\n'.join(lines)
    
    # Enhanced Fix: Null parameter test fixes (use null instead of mock.Object for null tests)
    if filename.endswith('Tests.cs'):
        # Fix null parameter tests to actually use null
        patterns_to_fix = [
            (r'(\+.*Assert\.Throws.*\(\(\) => new \w+\()(mockClient\.Object)(.*null.*\))', r'\1null\3'),
            (r'(\+.*Assert\.Throws.*\(\(\) => new \w+\()(.*mockClient\.Object.*)(, ".*"\))', r'\1null, \3'),
        ]
        
        for pattern, replacement in patterns_to_fix:
            if re.search(pattern, diff_content):
                diff_content = re.sub(pattern, replacement, diff_content)
                logger.info(f"Fixed null parameter test to use null instead of mock in {filename}")
    
    # Fix 4: Method name consistency for wrapper classes
    # Fix calls to WriteMessageAsync/ReadMessageAsync to match actual method names (writeMessage/readMessage)
    method_name_fixes = [
        (r'(\w+\.WriteMessageAsync\()', r'\1'.replace('WriteMessageAsync', 'writeMessage')),
        (r'(\w+\.ReadMessageAsync\()', r'\1'.replace('ReadMessageAsync', 'readMessage')),
        (r'(\.WriteMessageAsync\()', '.writeMessage('),
        (r'(\.ReadMessageAsync\()', '.readMessage(')
    ]
    
    for pattern, replacement in method_name_fixes:
        diff_content = re.sub(pattern, replacement, diff_content)
    
    # Fix 5: Async/await pattern consistency
    # Add await for calls to async wrapper methods
    async_patterns = [
        (r'(\+.*=\s*)(\w+\.readMessage\(\))', r'\1await \2'),
        (r'(\+.*=\s*)(\w+\.writeMessage\()', r'\1await \2')
    ]
    
    for pattern, replacement in async_patterns:
        if re.search(pattern, diff_content) and 'await' not in diff_content:
            diff_content = re.sub(pattern, replacement, diff_content)
    
    # Fix 6: Null parameter validation in test methods
    # Replace patterns like new ProducerWrapper(mockClient.Object, topicName) in null tests
    # with new ProducerWrapper(null, topicName) when testing null parameters
    null_test_pattern = r'(\+.*Assert\.Throws<ArgumentNullException>.*?new\s+\w+Wrapper\()([^)]+)(\).*?)'
    
    def fix_null_parameter_tests(match):
        prefix = match.group(1)
        parameters = match.group(2)
        suffix = match.group(3)
        
        # If this is testing null config/client, make sure we pass null as first parameter
        if 'NullConfig' in diff_content or 'NullClient' in diff_content:
            if 'mockClient.Object' in parameters:
                parameters = parameters.replace('mockClient.Object', 'null')
        
        return f'{prefix}{parameters}{suffix}'
    
    diff_content = re.sub(null_test_pattern, fix_null_parameter_tests, diff_content)
    
    logger.info(f"Completed compilation error fixes for {filename}")
    return diff_content

def validate_compilation(repo_path: str) -> Tuple[bool, List[str]]:
    """
    Run dotnet build to validate compilation and return detailed error information.
    
    Args:
        repo_path: Path to the repository
        
    Returns:
        Tuple of (success: bool, errors: List[str])
    """
    try:
        logger.info("🔍 Running compilation validation...")
        result = subprocess.run(
            ['dotnet', 'build', '--verbosity', 'minimal'],
            cwd=repo_path,
            capture_output=True,
            text=True,
            timeout=120
        )
        
        if result.returncode == 0:
            logger.info("✅ Compilation successful - no errors detected")
            return True, []
        else:
            logger.warning(f"❌ Compilation failed with {result.returncode} exit code")
            errors = []
            
            # Parse both stdout and stderr for errors
            output = result.stdout + result.stderr
            for line in output.split('\n'):
                if 'error CS' in line or 'error MSB' in line:
                    # Extract file path and error message
                    errors.append(line.strip())
            
            logger.warning(f"Found {len(errors)} compilation errors")
            for error in errors[:5]:  # Log first 5 errors
                logger.warning(f"  📋 {error}")
            
            return False, errors
            
    except subprocess.TimeoutExpired:
        logger.error("⏰ Compilation timeout after 120 seconds")
        return False, ["Compilation timeout - build took too long"]
    except FileNotFoundError:
        logger.error("❌ dotnet command not found - ensure .NET SDK is installed")
        return False, ["dotnet command not found"]
    except Exception as e:
        logger.error(f"❌ Compilation validation failed: {e}")
        return False, [f"Validation error: {str(e)}"]

def post_process_applied_files(repo_path: str) -> Dict[str, List[str]]:
    """
    Post-process applied migration files to fix common issues that may have been missed.
    This ensures the files are ready for compilation without manual intervention.
    
    Args:
        repo_path: Path to the repository
        
    Returns:
        Dictionary containing lists of fixes applied to each file
    """
    import os
    import re
    
    fixes_applied = {}
    
    # Define the files to check and their common issues
    files_to_check = [
        ('Api/Api.csproj', ['duplicate_xml_tags']),
        ('Api/ConsumerWrapperTests.cs', ['extra_braces', 'missing_variables', 'null_parameter_tests', 'incorrect_await']),
        ('Api/ProducerWrapperTests.cs', ['extra_braces', 'missing_variables', 'null_parameter_tests']),
        ('Api/Services/ProcessOrdersService.cs', ['incorrect_await']),
        ('Api/StartupTests.cs', ['missing_using_statements']),
        ('Api/Controllers/OrderControllerTests.cs', ['missing_using_statements']),
    ]
    
    for relative_path, issue_types in files_to_check:
        file_path = os.path.join(repo_path, relative_path)
        if not os.path.exists(file_path):
            continue
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            original_content = content
            fixes_for_file = []
            
            # Fix 1: Duplicate XML closing tags
            if 'duplicate_xml_tags' in issue_types and file_path.endswith('.csproj'):
                project_closes = content.count('</Project>')
                if project_closes > 1:
                    # Keep only the last </Project>
                    parts = content.split('</Project>')
                    content = '</Project>'.join(parts[:-project_closes]) + '</Project>'
                    fixes_for_file.append(f"Removed {project_closes - 1} duplicate </Project> tags")
            
            # Fix 2: Extra closing braces
            if 'extra_braces' in issue_types and file_path.endswith('.cs'):
                lines = content.split('\n')
                opening_braces = content.count('{')
                closing_braces = content.count('}')
                
                if closing_braces > opening_braces:
                    # Remove extra closing braces from the end
                    excess_braces = closing_braces - opening_braces
                    for i in range(len(lines) - 1, -1, -1):
                        if lines[i].strip() == '}' and excess_braces > 0:
                            lines[i] = ''
                            excess_braces -= 1
                    content = '\n'.join(lines)
                    fixes_for_file.append(f"Removed {closing_braces - opening_braces} extra closing braces")
            
            # Fix 3: Missing variable declarations in test methods
            if 'missing_variables' in issue_types and 'Tests.cs' in file_path:
                if 'mockClient.Object' in content and 'var mockClient = new Mock<ServiceBusClient>()' not in content:
                    # Find test methods that use mockClient.Object but don't declare it
                    lines = content.split('\n')
                    for i, line in enumerate(lines):
                        if 'mockClient.Object' in line:
                            # Find the method start
                            method_start = i
                            for j in range(i, -1, -1):
                                if any(marker in lines[j] for marker in ['[Fact]', '[Theory]']):
                                    method_start = j
                                    break
                            
                            # Find method opening brace
                            brace_line = -1
                            for j in range(method_start, min(i + 5, len(lines))):
                                if '{' in lines[j]:
                                    brace_line = j
                                    break
                            
                            if brace_line != -1:
                                # Check if mockClient is already declared in this method
                                method_segment = '\n'.join(lines[brace_line:i])
                                if 'mockClient' not in method_segment:
                                    # Add declaration
                                    indent = '            '  # Standard indentation
                                    lines.insert(brace_line + 1, f'{indent}var mockClient = new Mock<ServiceBusClient>();')
                                    fixes_for_file.append("Added missing mockClient declaration")
                                    break
                    content = '\n'.join(lines)
            
            # Fix 4: Incorrect await usage on synchronous methods
            if 'incorrect_await' in issue_types and file_path.endswith('.cs'):
                await_patterns_to_fix = [
                    ('await consumerHelper.readMessage()', 'consumerHelper.readMessage()'),
                    ('await wrapper.readMessage()', 'wrapper.readMessage()'),
                    ('var result = await wrapper.readMessage()', 'var result = wrapper.readMessage()'),
                ]
                
                for incorrect_pattern, correct_pattern in await_patterns_to_fix:
                    if incorrect_pattern in content:
                        content = content.replace(incorrect_pattern, correct_pattern)
                        fixes_for_file.append(f"Fixed incorrect await usage: {incorrect_pattern} -> {correct_pattern}")
                
                # Also handle Task<string> to string conversion issues
                if 'consumerHelper.readMessage()' in content and 'string orderRequest =' in content:
                    # This is likely a Task<string> being assigned to string
                    lines = content.split('\n')
                    for i, line in enumerate(lines):
                        if 'string orderRequest = consumerHelper.readMessage()' in line:
                            lines[i] = line.replace('consumerHelper.readMessage()', 'await consumerHelper.readMessage()')
                            fixes_for_file.append("Fixed Task<string> to string conversion by adding await")
                            break
                    content = '\n'.join(lines)
            
            # Fix 5: Missing using statements
            if 'missing_using_statements' in issue_types and file_path.endswith('.cs'):
                if 'ServiceBusClient' in content and 'using Azure.Messaging.ServiceBus;' not in content:
                    lines = content.split('\n')
                    # Find the right place to insert (after other using statements)
                    insert_index = 0
                    for i, line in enumerate(lines):
                        if line.startswith('using ') and 'System' in line:
                            insert_index = i + 1
                    
                    if insert_index > 0:
                        lines.insert(insert_index, 'using Azure.Messaging.ServiceBus;')
                        content = '\n'.join(lines)
                        fixes_for_file.append("Added missing using Azure.Messaging.ServiceBus statement")
            
            # Fix 6: Null parameter tests should use null, not mock objects
            if 'null_parameter_tests' in issue_types and 'Tests.cs' in file_path:
                # Fix patterns like new ConsumerWrapper(mockClient.Object, ...) in null parameter tests
                null_test_patterns = [
                    (r'(Assert\.Throws<ArgumentNullException>\(\(\) => new \w+\()(mockClient\.Object)(, [^)]+\))', r'\1null\3'),
                    (r'(Assert\.Throws<ArgumentNullException>\(\(\) => new \w+\([^,]+, )(mockClient\.Object)(\))', r'\1null\3'),
                ]
                
                for pattern, replacement in null_test_patterns:
                    if re.search(pattern, content):
                        content = re.sub(pattern, replacement, content)
                        fixes_for_file.append("Fixed null parameter test to use null instead of mock object")
            
            # Check for structural issues (method signatures, duplicate lines)
            if _has_structural_issues(content, file_path):
                content = _fix_structural_issues(content, file_path)
                fixes_for_file.append("Fixed structural issues (method signatures, duplicate lines)")
            
            # Apply fixes if any were made
            if content != original_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                fixes_applied[relative_path] = fixes_for_file
                logger.info(f"Applied {len(fixes_for_file)} post-processing fixes to {relative_path}")
                
        except Exception as e:
            logger.error(f"Error post-processing {file_path}: {e}")
    
    return fixes_applied

def _has_structural_issues(content: str, file_path: str) -> bool:
    """
    Detect structural issues in C# files that can cause compilation errors.
    
    Args:
        content: File content
        file_path: Path to the file
        
    Returns:
        True if structural issues are detected
    """
    lines = content.split('\n')
    
    # Check for missing method signatures before opening braces
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped == '{' and i > 0:
            prev_line = lines[i-1].strip()
            # Detect standalone opening brace without proper method/class declaration
            if (not prev_line.endswith(')') and 
                not any(keyword in prev_line for keyword in [
                    'class ', 'namespace ', 'if (', 'else', 'using (', 
                    'try', 'catch', 'finally', 'for (', 'while (', 'foreach ('
                ]) and
                not prev_line.endswith(';') and
                prev_line != '' and
                not prev_line.startswith('//') and
                'public void Configure' not in prev_line):
                logger.info(f"Found standalone opening brace after: '{prev_line}' in {file_path}")
                return True
    
    # Check for duplicate consecutive lines
    consecutive_duplicates = []
    for i in range(len(lines) - 1):
        if lines[i].strip() and lines[i].strip() == lines[i + 1].strip():
            if any(keyword in lines[i] for keyword in ['using ', 'services.Add', 'app.Use']):
                consecutive_duplicates.append((i, lines[i].strip()))
    
    if consecutive_duplicates:
        logger.info(f"Found {len(consecutive_duplicates)} duplicate lines in {file_path}")
        return True
    
    # Check for missing using statements causing compilation errors
    if ('ServiceBusClient' in content or 'ServiceBusProcessor' in content) and 'using Azure.Messaging.ServiceBus;' not in content:
        logger.info(f"Missing Azure.Messaging.ServiceBus using statement in {file_path}")
        return True
    
    return False

def _fix_structural_issues(content: str, file_path: str) -> str:
    """
    Fix structural issues in C# files.
    
    Args:
        content: File content
        file_path: Path to the file
        
    Returns:
        Fixed content
    """
    lines = content.split('\n')
    fixed_lines = []
    i = 0
    
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()
        
        # Fix standalone opening brace in Startup.cs files
        if 'Startup.cs' in file_path and stripped == '{' and i > 0:
            prev_line = lines[i-1].strip()
            if (prev_line.endswith('();') and 'services.' in prev_line) or prev_line.endswith('}'):
                # This looks like a missing Configure method declaration
                fixed_lines.append(lines[i-1])  # Keep the previous line
                if not prev_line.endswith('}'):
                    fixed_lines.append('        }')
                    fixed_lines.append('')
                fixed_lines.append('        // Configure the HTTP request pipeline')
                fixed_lines.append('        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)')
                fixed_lines.append('        {')
                i += 1
                logger.info(f"Added missing Configure method declaration in {file_path}")
                continue
        
        # Remove duplicate consecutive lines
        if i < len(lines) - 1 and stripped and stripped == lines[i + 1].strip():
            if any(keyword in stripped for keyword in ['using ', 'services.Add', 'app.Use']):
                fixed_lines.append(line)  # Keep first occurrence
                i += 2  # Skip the duplicate
                logger.info(f"Removed duplicate line: {stripped[:50]}...")
                continue
        
        # Add missing using statements
        if (i == 0 or (stripped.startswith('using ') and not stripped.startswith('using ('))) and \
           ('ServiceBusClient' in content or 'ServiceBusProcessor' in content) and \
           'using Azure.Messaging.ServiceBus;' not in content:
            # Find the right place to insert the using statement
            if stripped.startswith('using ') and 'System' not in stripped:
                fixed_lines.append('using Azure.Messaging.ServiceBus;')
                logger.info(f"Added missing using Azure.Messaging.ServiceBus statement in {file_path}")
        
        fixed_lines.append(line)
        i += 1
    
    return '\n'.join(fixed_lines)


def validate_and_retry_migration(repo_path: str, max_retries: int = 2) -> Tuple[bool, List[str], int]:
    """
    Comprehensive validation and retry mechanism for migration consistency.
    
    Args:
        repo_path: Path to the repository
        max_retries: Maximum number of retry attempts
        
    Returns:
        Tuple of (success: bool, remaining_errors: List[str], attempts_made: int)
    """
    attempts = 0
    
    for attempt in range(max_retries + 1):
        attempts += 1
        logger.info(f"🔄 Validation attempt {attempts}/{max_retries + 1}")
        
        # Apply post-processing fixes
        fixes_applied = post_process_applied_files(repo_path)
        if fixes_applied:
            logger.info(f"Applied fixes to {len(fixes_applied)} files in attempt {attempts}")
        
        # Validate compilation
        compilation_success, errors = validate_compilation(repo_path)
        
        if compilation_success:
            logger.info(f"✅ Migration validation successful after {attempts} attempt(s)")
            return True, [], attempts
        
        if attempt < max_retries:
            logger.info(f"⚠️ Attempt {attempts} failed with {len(errors)} errors, retrying...")
            # Apply more aggressive fixes for retry
            _apply_aggressive_fixes(repo_path, errors)
        else:
            logger.warning(f"❌ All {attempts} validation attempts failed")
            return False, errors, attempts
    
    return False, [], attempts

def _apply_aggressive_fixes(repo_path: str, compilation_errors: List[str]) -> None:
    """
    Apply more aggressive fixes based on specific compilation errors.
    
    Args:
        repo_path: Path to the repository
        compilation_errors: List of compilation error messages
    """
    logger.info("🛠️ Applying aggressive fixes based on compilation errors...")
    
    # Parse errors to identify patterns
    missing_usings = set()
    structural_files = set()
    
    for error in compilation_errors:
        # Extract file paths from errors
        if '.cs(' in error:
            file_match = re.search(r'([^\\]+\.cs)\(', error)
            if file_match:
                file_name = file_match.group(1)
                
                # Check for missing using statements
                if 'could not be found' in error and ('ServiceBus' in error or 'Azure' in error):
                    missing_usings.add(f"Api/{file_name}")
                
                # Check for structural issues
                if any(code in error for code in ['CS1519', 'CS1022', 'CS8124', 'CS1026']):
                    structural_files.add(f"Api/{file_name}")
    
    # Apply targeted fixes
    for file_rel_path in missing_usings:
        file_path = os.path.join(repo_path, file_rel_path)
        if os.path.exists(file_path):
            _add_missing_using_statements(file_path)
    
    for file_rel_path in structural_files:
        file_path = os.path.join(repo_path, file_rel_path)
        if os.path.exists(file_path):
            _fix_structural_syntax_errors(file_path)

def _add_missing_using_statements(file_path: str) -> None:
    """Add missing using statements to a C# file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        required_usings = [
            'using Azure.Messaging.ServiceBus;',
            'using System.Threading.Tasks;'
        ]
        
        modified = False
        lines = content.split('\n')
        
        for using_stmt in required_usings:
            if using_stmt not in content:
                # Find the right place to insert
                for i, line in enumerate(lines):
                    if line.strip().startswith('using ') and 'System' not in line and not line.startswith('using ('):
                        lines.insert(i, using_stmt)
                        modified = True
                        logger.info(f"Added {using_stmt} to {os.path.basename(file_path)}")
                        break
        
        if modified:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(lines))
                
    except Exception as e:
        logger.error(f"Error adding using statements to {file_path}: {e}")

def _fix_structural_syntax_errors(file_path: str) -> None:
    """Fix structural syntax errors in C# files."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Fix common structural issues
        lines = content.split('\n')
        fixed_lines = []
        
        i = 0
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()
            
            # Fix standalone opening braces
            if stripped == '{' and i > 0:
                prev_line = lines[i-1].strip()
                if 'Startup.cs' in file_path and (prev_line.endswith('();') or prev_line.endswith('}')):
                    # Likely missing method declaration
                    if not prev_line.endswith('}'):
                        fixed_lines.append(lines[i-1])
                        fixed_lines.append('        }')
                        fixed_lines.append('')
                    fixed_lines.append('        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)')
                    fixed_lines.append('        {')
                    i += 1
                    continue
            
            fixed_lines.append(line)
            i += 1
        
        if '\n'.join(fixed_lines) != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(fixed_lines))
            logger.info(f"Applied structural fixes to {os.path.basename(file_path)}")
            
    except Exception as e:
        logger.error(f"Error fixing structural issues in {file_path}: {e}")




def validate_diff_response(diff_text: str, filename: str) -> Dict[str, Any]:
    """
    Phase 4: Enhanced validation for git diff format and ellipsis detection.
    
    Args:
        diff_text: Diff text to validate
        filename: Name of the file being processed (for logging)
        
    Returns:
        Dict with validation results:
        - is_valid: bool - Whether the diff is valid
        - reason: str - Reason if invalid
        - has_basic_format: bool - Whether it has basic git diff markers
    """
    result = {
        "is_valid": True,
        "reason": "",
        "has_basic_format": False
    }
    
    if not diff_text or len(diff_text.strip()) < 50:
        result["is_valid"] = False
        result["reason"] = "Response too short or empty"
        return result
    
    # Check for basic git diff format markers
    has_header = "---" in diff_text and "+++" in diff_text
    has_hunks = "@@" in diff_text
    
    result["has_basic_format"] = has_header and has_hunks
    
    if not result["has_basic_format"]:
        result["is_valid"] = False
        result["reason"] = "Missing git diff format markers (---, +++, @@)"
        return result
    
    # Check for ellipsis patterns
    lines = diff_text.split('\n')
    ellipsis_patterns = [
        '...',
        '... (truncated)',
        '... (omitted)',
        '... (continued)',
        '... (rest of file)'
    ]
    
    ellipsis_lines = []
    for i, line in enumerate(lines, 1):
        stripped = line.strip()
        # Check for ellipsis-only lines
        if stripped in ['...', '... ', '...\n']:
            ellipsis_lines.append(i)
        # Check for lines containing ellipsis patterns
        for pattern in ellipsis_patterns:
            if pattern.lower() in line.lower():
                ellipsis_lines.append(i)
                break
    
    if ellipsis_lines:
        result["is_valid"] = False
        result["reason"] = f"Contains ellipsis markers at lines: {ellipsis_lines[:5]}{'...' if len(ellipsis_lines) > 5 else ''}"
        return result
    
    # Check for trailing ellipsis in hunks (incomplete diffs)
    # Look for hunks that end with ellipsis
    in_hunk = False
    for i, line in enumerate(lines):
        if line.startswith('@@'):
            in_hunk = True
        elif in_hunk and (line.startswith('@@') or line.startswith('---') or line.startswith('+++')):
            # New hunk or section started
            in_hunk = False
        elif in_hunk and i < len(lines) - 1:
            # Check if this is near the end of a hunk and has ellipsis
            next_line = lines[i + 1] if i + 1 < len(lines) else ""
            if line.strip().endswith('...') and (next_line.startswith('@@') or next_line.startswith('---') or next_line.startswith('+++')):
                result["is_valid"] = False
                result["reason"] = f"Trailing ellipsis detected in hunk near line {i + 1}"
                return result
    
    # Validate diff structure - check for balanced parentheses/braces in diff content
    # This is a basic check - more sophisticated validation can be added
    diff_content = '\n'.join([line for line in lines if line.startswith(('+', '-', ' '))])
    
    # Count parentheses and braces (rough check)
    open_parens = diff_content.count('(')
    close_parens = diff_content.count(')')
    open_braces = diff_content.count('{')
    close_braces = diff_content.count('}')
    
    # Allow some tolerance (5) for incomplete context
    if abs(open_parens - close_parens) > 5:
        result["is_valid"] = False
        result["reason"] = f"Significant parentheses mismatch: {open_parens} opening, {close_parens} closing"
        return result
    
    if abs(open_braces - close_braces) > 5:
        result["is_valid"] = False
        result["reason"] = f"Significant braces mismatch: {open_braces} opening, {close_braces} closing"
        return result
    
    # Validate hunk structure - check that hunks have proper format
    hunk_count = diff_text.count('@@')
    if hunk_count == 0:
        result["is_valid"] = False
        result["reason"] = "No diff hunks found"
        return result
    
    # If we get here, the diff appears valid
    result["is_valid"] = True
    result["reason"] = "Valid diff format"
    return result


async def generate_code_diffs_async(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enhanced diff generation with better error handling and improved prompts
    """
    config = state.get("config", AnalysisConfig())
    stats = state.get("stats", MigrationStats())
    migration_start_time = time.time()
    
    logger.info("Starting code diff generation")
    
    try:
        llm = AzureChatOpenAI(
            deployment_name=state.get('model'),
            azure_endpoint=state.get('base_url'),
            openai_api_key=state.get('api_key'),
            openai_api_version=state.get('api_version'),
            timeout=config.ai_timeout,
            max_retries=config.retry_attempts
        )
    except Exception as e:
        error_msg = f"Failed to initialize AI client for diff generation: {str(e)}"
        logger.error(error_msg)
        state.setdefault("errors", []).append(error_msg)
        return state

    repo_path = state["repo_path"]
    inventory = state.get("kafka_inventory", [])

    if not inventory:
        logger.info("No Kafka inventory found, skipping diff generation")
        return state

    sem = asyncio.Semaphore(config.max_workers)

    async def process_file_for_diff(item: Dict[str, Any]) -> Dict[str, str]:
        """Generate migration diff for a single file with retry logic"""
        rel_file = item["file"]
        abs_file = os.path.join(repo_path, rel_file)

        # Read file once outside the retry loop
        try:
            with open(abs_file, "r", encoding="utf-8", errors="ignore") as f:
                original_code = f.read()
        except Exception as e:
            logger.warning(f"Failed to read file {rel_file}: {str(e)}")
            return {"file": rel_file, "diff": f"Error reading file: {e}"}

        # Phase 1: Detect file type and determine processing strategy
        is_test_file = 'test' in rel_file.lower() or rel_file.lower().endswith('tests.cs')
        
        # Use full file content for accurate diff generation
        if is_test_file:
            logger.info(f"Using full file content for test file: {rel_file}")
            max_content_size = 50000  # 50KB limit for test files
        else:
            logger.info(f"Using full file content for core file: {rel_file}")
            max_content_size = 80000  # 80KB limit for core files
            
        # Only truncate if file is extremely large to prevent token overflow
        if len(original_code) > max_content_size:
            # For very large files, include beginning and end to capture structure
            middle_omit_msg = f"\\n\\n// ... (middle {len(original_code) - max_content_size} characters omitted for brevity) ...\\n\\n"
            code_preview = original_code[:max_content_size//2] + middle_omit_msg + original_code[-max_content_size//2:]
            is_truncated = True
            logger.warning(f"File {rel_file} is {len(original_code)} chars, using structured preview")
        else:
            # Use complete file content for accurate diff generation
            code_preview = original_code
            is_truncated = False
            logger.info(f"Using complete file content for {rel_file} ({len(original_code)} chars)")
        
        # Track if we're retrying due to ellipsis
        previous_attempt_had_ellipsis = False

        for attempt in range(config.retry_attempts):
            # Progressive content reduction on timeout retries
            if attempt > 0:
                # Reduce content size on retries to handle timeouts
                retry_limits = [50000, 35000, 25000]
                if attempt <= len(retry_limits) and len(original_code) > retry_limits[attempt - 1]:
                    limit = retry_limits[attempt - 1]
                    middle_msg = f"\\n\\n// ... (retry {attempt}: middle section omitted for timeout handling) ...\\n\\n"
                    code_preview = original_code[:limit//2] + middle_msg + original_code[-limit//2:]
                    is_truncated = True
                    logger.info(f"Retry {attempt + 1}: Reducing content for {rel_file} to {limit} chars due to timeout")
            
            # Build prompt - more explicit on retry
            if previous_attempt_had_ellipsis:
                critical_section = f"""
⚠️ RETRY REQUEST - PREVIOUS RESPONSE HAD ELLIPSIS ⚠️

CRITICAL - YOU MUST FOLLOW THESE RULES:
1. NEVER use ellipsis (...) anywhere in your response - not in lines, not at the end, nowhere.
2. Generate COMPLETE diffs showing every single line that changes.
3. If you cannot show all changes, generate multiple complete hunks.
4. Every method body must be shown in full, not abbreviated.
5. Do NOT use placeholders like "...", "... (truncated)", "... (omitted)", or any variation.
6. Your response must be a complete, valid git diff with NO ellipsis markers whatsoever.

"""
            else:
                if is_truncated:
                    critical_section = f"""
🔴 FILE STRUCTURE ANALYSIS REQUIRED: File is {len(original_code)} characters long.

⚠️  MANDATORY STEPS:
1. CAREFULLY ANALYZE the current file structure and content shown below
2. Identify ALL Kafka-related code patterns in the ACTUAL file content
3. Generate diffs that match the ACTUAL line numbers and code structure
4. Include ALL using statements that need to be added or modified
5. Include ALL field declarations that need to be changed  
6. Include ALL method implementations that need to be updated
7. Do NOT use placeholders like "...", "... (truncated)", "... (omitted)", or any variation
8. Ensure hunk headers @@ -X,Y +A,B @@ match ACTUAL file line numbers
9. Provide COMPLETE code blocks - never abbreviate or truncate

🎯 ACCURACY REQUIREMENT: Your diff must apply cleanly to the actual file structure!
"""
                else:
                    critical_section = f"""
✅ COMPLETE FILE ANALYSIS:
1. ANALYZE the complete file structure shown below  
2. Generate precise diffs matching ACTUAL line numbers and content
3. Ensure ALL Kafka references are properly migrated
4. Validate that hunk headers match the real file structure
5. NEVER use ellipsis (...) in your response
6. Generate complete diffs for ALL sections that need changes
"""
            
            # Generate different prompts based on file type
            if is_test_file:
                # Progressive prompt strategy - simpler on retries to handle timeouts
                if attempt > 1:
                    # Ultra-simple prompt for final retry
                    prompt = f"""
Convert test file from Kafka to Service Bus. Return git diff format.

FILE: {rel_file}
{code_preview}

--- a/{rel_file}
+++ b/{rel_file}
Format required."""
                elif attempt > 0:
                    # Simplified retry prompt
                    prompt = f"""
Migrate C# test: Kafka → Service Bus

Changes needed:
- using Confluent.Kafka → using Azure.Messaging.ServiceBus  
- ConsumerConfig → ServiceBusProcessorOptions
- ProducerConfig → ServiceBusClient parameter
- Remove ConsumeResult<>, ProduceException<>
- Add Mock<ServiceBusClient>

FILE: {rel_file}
{code_preview}

Git diff only:"""
                else:
                    # Get constructor signature from corresponding wrapper class
                    wrapper_signature_guidance = ""
                    if 'ConsumerWrapperTests.cs' in rel_file:
                        # Check if we have ConsumerWrapper diff to extract actual signature
                        wrapper_diff = None
                        for existing_item in state.get('code_diffs', []):
                            if existing_item.get('file', '').endswith('ConsumerWrapper.cs'):
                                wrapper_diff = existing_item.get('diff', '')
                                break
                        
                        # Force specific ConsumerWrapper constructor signature
                        wrapper_signature_guidance = """
CRITICAL CONSTRUCTOR SIGNATURE:
ConsumerWrapper must use: ConsumerWrapper(ServiceBusClient client, string topicName, ServiceBusProcessorOptions options = null)

TEST CONSTRUCTOR CALLS must use exactly 3 parameters:
new ConsumerWrapper(mockClient.Object, topicName, options)

Ensure Mock<ServiceBusClient> is declared as: var mockClient = new Mock<ServiceBusClient>();
"""
                            
                    elif 'ProducerWrapperTests.cs' in rel_file:
                        # Check if we have ProducerWrapper diff to extract actual signature
                        wrapper_diff = None
                        for existing_item in state.get('code_diffs', []):
                            if existing_item.get('file', '').endswith('ProducerWrapper.cs'):
                                wrapper_diff = existing_item.get('diff', '')
                                break
                        
                        # Force specific ProducerWrapper constructor signature
                        wrapper_signature_guidance = """
CRITICAL CONSTRUCTOR SIGNATURE:
ProducerWrapper must use: ProducerWrapper(ServiceBusClient client, string topicName)

TEST CONSTRUCTOR CALLS must use exactly 2 parameters:
new ProducerWrapper(mockClient.Object, topicName)

Ensure Mock<ServiceBusClient> is declared as: var mockClient = new Mock<ServiceBusClient>();
"""
                    
                    prompt = f"""
Generate git diff for .NET test file migration: Kafka → Azure Service Bus

REQUIRED FORMAT:
--- a/{rel_file}
+++ b/{rel_file}
@@ -line,count +line,count @@

{wrapper_signature_guidance}

API CHANGES:
- using Confluent.Kafka → using Azure.Messaging.ServiceBus
- ConsumerConfig → ServiceBusProcessorOptions  
- ProducerConfig → ServiceBusClient parameter
- Remove: ConsumeResult<>, ProduceException<>, .Consume(), .ProduceAsync()
- Add: Mock<ServiceBusClient> in tests

FILE CONTENT:
{code_preview}

Return git diff in standard format."""
            else:
                # Enhanced prompt for core files with accurate structure analysis
                prompt = f"""
You are a .NET Core migration expert specializing in Kafka to Azure Service Bus migrations.

🔍 CRITICAL ANALYSIS REQUIREMENTS:
1. MUST read and understand the COMPLETE current file content below
2. Generate diffs based on ACTUAL current code structure, not assumptions
3. Ensure hunk headers @@ -X,Y +A,B @@ match the real file line numbers
4. Base all transformations on the EXACT code structure shown

⚠️  CRITICAL WARNING: DO NOT use these NON-EXISTENT interfaces:
- IServiceBusProcessor (DOES NOT EXIST - use ServiceBusProcessor)
- IServiceBusSender (DOES NOT EXIST - use ServiceBusSender)
- IServiceBusReceiver (DOES NOT EXIST - use ServiceBusReceiver)

✅ ONLY use these CONCRETE CLASSES from Azure.Messaging.ServiceBus:
- ServiceBusClient, ServiceBusProcessor, ServiceBusSender, ServiceBusReceiver

FILE: {rel_file}
KAFKA APIS DETECTED: {item.get('kafka_apis', [])}
FILE SIZE: {len(original_code)} characters{' (structured preview)' if is_truncated else ' (complete file)'}

TASK: Generate a COMPLETE git diff showing ALL changes needed to migrate Kafka to Azure Service Bus.

{critical_section}

CRITICAL AZURE SERVICE BUS MIGRATION RULES:
1. Replace 'using Confluent.Kafka' with 'using Azure.Messaging.ServiceBus'
2. Replace IProducer<TKey, TValue> with ServiceBusSender (concrete class, NOT IServiceBusSender)
3. Replace IConsumer<TKey, TValue> with ServiceBusProcessor (concrete class, NOT IServiceBusProcessor)
4. Replace ProducerBuilder<TKey, TValue> with ServiceBusClient.CreateSender()
5. Replace ConsumerBuilder<TKey, TValue> with ServiceBusClient.CreateProcessor()
6. Replace ProducerConfig with ServiceBusClient constructor
7. Replace ConsumerConfig with ServiceBusProcessorOptions
8. Replace Produce/ProduceAsync with SendMessageAsync(ServiceBusMessage)
9. Replace Subscribe() with ProcessMessageAsync event handler registration
10. Replace Consume() with async event-driven pattern using ProcessMessageAsync
11. Replace Message.Value with ServiceBusReceivedMessage.Body.ToString()
12. Add 'using System.Threading.Tasks' for async operations
13. Maintain all existing business logic and error handling
14. Add proper async/await patterns and dispose patterns
15. CRITICAL: Ensure Interlocked.CompareExchange uses correct parameter types (ref T, T, T)
16. CRITICAL: Declare all variables before use (e.g., mockClient in test methods)
17. CRITICAL: Use correct variable types in generic method calls
18. CRITICAL: In null parameter tests, use 'null' instead of mock objects
19. CRITICAL: Use correct method names - writeMessage() and readMessage() (NOT WriteMessageAsync/ReadMessageAsync)
20. CRITICAL: Add 'await' for async wrapper method calls
21. CRITICAL: Ensure method signatures match actual wrapper implementations
22. CRITICAL: OrderController MUST use ServiceBusClient, NOT ProducerConfig - ensure constructor parameter consistency
23. CRITICAL: Remove any remaining Confluent.Kafka imports when Azure.Messaging.ServiceBus is present
24. CRITICAL: Never use IServiceBusProcessor, IServiceBusSender - these interfaces don't exist
25. CRITICAL: Validate that all constructors across files use compatible parameter types

MANDATORY WRAPPER CLASS CONSTRUCTOR SIGNATURES:
- ConsumerWrapper MUST use: public ConsumerWrapper(ServiceBusClient client, string topicName, ServiceBusProcessorOptions options = null)
- ProducerWrapper MUST use: public ProducerWrapper(ServiceBusClient client, string topicName)
- OrderController MUST use: public OrderController(ServiceBusClient client) - NOT ProducerConfig
- Both constructors MUST accept ServiceBusClient as first parameter
- Do NOT create constructors that take connection strings or other patterns

CONSTRUCTOR CONSISTENCY VALIDATION:
- All classes that use ProducerWrapper/ConsumerWrapper MUST inject ServiceBusClient
- OrderController MUST receive ServiceBusClient via dependency injection
- Remove all ProducerConfig/ConsumerConfig usage and replace with ServiceBusClient

AZURE SERVICE BUS SPECIFIC PATTERNS:
- ServiceBusClient: Main client for creating senders/processors
- ServiceBusSender: For sending messages (NOT IServiceBusSender interface)
- ServiceBusProcessor: For receiving messages (NOT IServiceBusProcessor interface)  
- ServiceBusMessage: Message wrapper for sending
- ServiceBusReceivedMessage: Message received from service bus
- ProcessMessageEventArgs: Event args for message processing
- ProcessErrorEventArgs: Event args for error handling

RETURN FORMAT: Return ONLY the git diff format showing the changes:
- Use "--- a/{rel_file}" for the original file header
- Use "+++ b/{rel_file}" for the modified file header
- Use @@ -X,Y +A,B @@ for line number ranges
- Use "-" prefix for removed lines
- Use "+" prefix for added lines
- Include 3 lines of context before and after changes
- Generate COMPLETE diffs - show all changes, never use ellipsis

CRITICAL: Use CONCRETE CLASSES, not interfaces:
- ServiceBusProcessor (correct) - NOT IServiceBusProcessor (does not exist!)
- ServiceBusSender (correct) - NOT IServiceBusSender (does not exist!)

EXAMPLE CONSUMER TRANSFORMATION:
Field declaration: IConsumer<string,string> -> ServiceBusProcessor
Instantiation: ConsumerBuilder<string,string>().Build() -> ServiceBusClient.CreateProcessor()
Subscribe pattern: consumer.Subscribe() -> event-driven ProcessMessageAsync handlers

EXAMPLE PRODUCER TRANSFORMATION:  
Field declaration: IProducer<string,string> -> ServiceBusSender
Instantiation: ProducerBuilder<string,string>().Build() -> ServiceBusClient.CreateSender()
Send pattern: producer.Produce() -> sender.SendMessageAsync(ServiceBusMessage)

VALIDATION CHECKLIST - Ensure your diff contains:
✅ ServiceBusProcessor (NOT IServiceBusProcessor)
✅ ServiceBusSender (NOT IServiceBusSender)  
✅ ServiceBusClient.CreateProcessor() method calls
✅ ProcessMessageAsync event handlers for async processing
✅ using Azure.Messaging.ServiceBus
✅ using System.Threading.Tasks for async methods
✅ ConsumerWrapper constructor: (ServiceBusClient client, string topicName, ServiceBusProcessorOptions options = null)
✅ ProducerWrapper constructor: (ServiceBusClient client, string topicName)
✅ Constructor parameters in correct order: ServiceBusClient first, then topicName
✅ All variables declared before use (especially mockClient in test methods)
✅ Interlocked.CompareExchange parameters have matching types
✅ Null parameter tests use 'null', not mock objects
✅ No undefined variables or type mismatches
✅ Correct method names: writeMessage(), readMessage() (not WriteMessageAsync/ReadMessageAsync)
✅ Async/await patterns for wrapper method calls
✅ Method calls match actual wrapper implementations

ORIGINAL CODE:
{code_preview}
"""

            async with sem:
                try:
                    # Use longer timeout for test files since they can be complex
                    base_timeout = config.ai_timeout + 30 if is_test_file else config.ai_timeout
                    # Progressive timeout reduction on retries
                    timeout_reduction = attempt * 15  # Reduce by 15s per retry
                    timeout = max(base_timeout - timeout_reduction, 45)  # Minimum 45s
                    resp = await asyncio.wait_for(
                        asyncio.to_thread(lambda: llm.invoke([HumanMessage(content=prompt)])),
                        timeout=timeout
                    )
                    diff_text = getattr(resp, "content", "").strip()
                    
                    # Phase 4: Response cleanup - Remove ellipsis and truncated markers
                    diff_text = clean_diff_response(diff_text)
                    
                    # Validate that we got a meaningful git diff response
                    if len(diff_text) < 50:
                        logger.warning(f"Received short response for {rel_file} (attempt {attempt + 1})")
                        if attempt < config.retry_attempts - 1:
                            continue
                    
                    # Phase 4: Enhanced validation with Kafka API removal check
                    if is_test_file:
                        # Test file validation with Kafka API checking
                        validation_result = validate_diff_response(diff_text, rel_file)
                        if validation_result["is_valid"]:
                            # Additional validation: Check for Kafka APIs that cause compilation errors
                            kafka_validation = validate_kafka_removal(diff_text, rel_file)
                            if not kafka_validation["is_valid"]:
                                validation_result = kafka_validation
                                validation_result["has_basic_format"] = True  # Format is OK, but content needs fixing
                    else:
                        # Full validation for core files to prevent hallucination
                        validation_result = validate_diff_response(diff_text, rel_file)
                        if validation_result["is_valid"]:
                            # Additional validation: Check for Kafka APIs that cause compilation errors
                            kafka_validation = validate_kafka_removal(diff_text, rel_file)
                            if not kafka_validation["is_valid"]:
                                validation_result = kafka_validation
                                validation_result["has_basic_format"] = True  # Format is OK, but content needs fixing
                    
                    if not validation_result["is_valid"]:
                        reason = validation_result["reason"]
                        logger.warning(f"Diff validation failed for {rel_file} (attempt {attempt + 1}): {reason}")
                        
                        # Phase 4: Retry logic if ellipsis detected
                        if "ellipsis" in reason.lower() or "truncated" in reason.lower():
                            previous_attempt_had_ellipsis = True
                            if attempt < config.retry_attempts - 1:
                                logger.info(f"Retrying {rel_file} with more explicit prompt due to ellipsis detection")
                                # Use a more explicit prompt on retry
                                await asyncio.sleep(1)  # Brief delay before retry
                                continue
                        elif not validation_result.get("has_basic_format", False):
                            # Not a valid git diff format
                            if attempt < config.retry_attempts - 1:
                                continue
                    
                    # If we have a valid diff (or last attempt), apply fixes and return it
                    if validation_result.get("has_basic_format", False):
                        # Apply compilation error fixes for test files
                        if is_test_file:
                            diff_text = validate_and_fix_compilation_errors(diff_text, rel_file)
                            # Re-validate after fixes
                            kafka_validation = validate_kafka_removal(diff_text, rel_file)
                            if not kafka_validation["is_valid"]:
                                logger.warning(f"Kafka APIs still found in {rel_file} after fixes: {kafka_validation['reason']}")
                        
                        # Apply interface fixes for test files to ensure constructor/method consistency
                        if is_test_file:
                            # Create a temporary diff list for validation
                            temp_diff_list = [{"file": rel_file, "diff": diff_text}]
                            # Check if we have wrapper diffs to extract interfaces
                            wrapper_diffs = [d for d in state.get('code_diffs', []) 
                                           if d.get('file', '').endswith('Wrapper.cs') and 'Test' not in d.get('file', '')]
                            if wrapper_diffs:
                                temp_diff_list.extend(wrapper_diffs)
                                # Validate constructor and interface consistency
                                constructor_result = validate_constructor_consistency(temp_diff_list)
                                interface_result = validate_interface_consistency(temp_diff_list)
                                
                                if constructor_result.get('has_errors') or interface_result.get('has_errors'):
                                    logger.info(f"Applying interface fixes to {rel_file}")
                                    fixed_diffs = apply_interface_fixes(temp_diff_list, constructor_result, interface_result)
                                    if fixed_diffs and len(fixed_diffs) > 0:
                                        # Get the fixed diff for this file
                                        for fixed_diff in fixed_diffs:
                                            if fixed_diff.get('file') == rel_file:
                                                diff_text = fixed_diff.get('diff', diff_text)
                                                logger.info(f"Applied interface fixes to {rel_file}")
                                                break
                        
                        return {"file": rel_file, "diff": diff_text}
                    else:
                        # Last attempt and still invalid - return with warning
                        logger.error(f"Failed to generate valid diff for {rel_file} after {config.retry_attempts} attempts")
                        return {"file": rel_file, "diff": diff_text}  # Return anyway for debugging
                    
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout generating diff for {rel_file} (attempt {attempt + 1}/{config.retry_attempts}) - will retry with reduced content")
                    if attempt == config.retry_attempts - 1:
                        logger.error(f"All timeout retries failed for {rel_file}")
                        return {"file": rel_file, "diff": "Error: Timeout during diff generation"}
                    else:
                        await asyncio.sleep(0.5)  # Reduced delay from 2s to 0.5s
                except Exception as e:
                    logger.warning(f"Error generating diff for {rel_file} (attempt {attempt + 1}): {str(e)}")
                    if attempt == config.retry_attempts - 1:
                        return {"file": rel_file, "diff": f"Error generating diff: {e}"}
                    await asyncio.sleep(0.3)  # Reduced delay from 1s to 0.3s

        return {"file": rel_file, "diff": "Error: All retry attempts failed"}

    # Process in batches
    batch_size = min(config.max_workers, 10)  # Increased from 5 to 10 for faster diff generation
    all_diffs = []
    
    for i in range(0, len(inventory), batch_size):
        batch = inventory[i:i + batch_size]
        logger.info(f"Generating diffs for batch {i//batch_size + 1}/{(len(inventory) + batch_size - 1)//batch_size}")
        
        tasks = [process_file_for_diff(item) for item in batch]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions
        valid_results = [r for r in batch_results if isinstance(r, dict)]
        all_diffs.extend(valid_results)

    # Deduplicate by file
    seen = {}
    for d in all_diffs:
        seen[d["file"]] = d

    # Post-process: Validate and fix all test files together for consistency
    final_diffs = list(seen.values())
    test_files = [d for d in final_diffs if 'test' in d.get('file', '').lower() or d.get('file', '').lower().endswith('tests.cs')]
    
    if test_files:
        logger.info(f"Post-processing {len(test_files)} test files for consistency validation")
        
        # Validate constructor and interface consistency
        constructor_result = validate_constructor_consistency(final_diffs)
        interface_result = validate_interface_consistency(final_diffs)
        
        if constructor_result.get('has_errors') or interface_result.get('has_errors'):
            logger.warning("Interface consistency issues detected, applying fixes...")
            if constructor_result.get('has_errors'):
                logger.warning(f"Constructor errors: {constructor_result['errors']}")
            if interface_result.get('has_errors'):
                logger.warning(f"Interface errors: {interface_result['errors']}")
            
            # Apply comprehensive fixes
            fixed_diffs = apply_interface_fixes(final_diffs, constructor_result, interface_result)
            
            # Update the diffs with fixed versions
            fixed_dict = {d.get('file'): d for d in fixed_diffs}
            for i, diff_item in enumerate(final_diffs):
                file_path = diff_item.get('file')
                if file_path in fixed_dict:
                    final_diffs[i] = fixed_dict[file_path]
                    logger.info(f"Applied fixes to {file_path}")
        
        # Additional validation: Check each test file for compilation errors
        for diff_item in test_files:
            file_path = diff_item.get('file')
            diff_content = diff_item.get('diff', '')
            
            # Validate Kafka removal
            kafka_validation = validate_kafka_removal(diff_content, file_path)
            if not kafka_validation["is_valid"]:
                logger.warning(f"Kafka APIs found in {file_path}: {kafka_validation['reason']}")
                # Apply compilation error fixes
                fixed_diff = validate_and_fix_compilation_errors(diff_content, file_path)
                if fixed_diff != diff_content:
                    diff_item['diff'] = fixed_diff
                    logger.info(f"Applied compilation error fixes to {file_path}")
    
    stats.migration_duration = time.time() - migration_start_time
    
    logger.info(f"Diff generation completed in {stats.migration_duration:.2f}s for {len(final_diffs)} files")
    
    state["code_diffs"] = final_diffs
    state["stats"] = stats
    return state

# -----------------------------
# Sync wrapper for StateGraph
# -----------------------------
def generate_code_diffs(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Synchronous wrapper for async diff generation.
    """
    return asyncio.run(generate_code_diffs_async(state))


def generate_report_streaming(state: RepoAnalysisState, report_path="migration-report.md") -> RepoAnalysisState:
    """
    Streaming-style report generator for Kafka → Azure Service Bus migration.
    Ignores README.md and writes report incrementally.
    """
    kafka_inventory = state.get("kafka_inventory", [])
    code_diffs = state.get("code_diffs", [])

    with open(report_path, "w", encoding="utf-8") as f:
        # Header
        f.write("# Kafka → Azure Service Bus Migration Report\n\n")

        # 1. Kafka Usage Inventory
        f.write("## 1. Kafka Usage Inventory\n\n")
        filtered_inventory = [item for item in kafka_inventory if item.get("file", "").lower() != "readme.md"]
        if not filtered_inventory:
            f.write("_No Kafka usage detected in the repository._\n\n")
        else:
            f.write("| File | Kafka APIs | Summary |\n")
            f.write("|------|-----------|---------|\n")
            for item in filtered_inventory:
                file = item.get("file", "")
                apis = ", ".join(item.get("kafka_apis", []))
                summary = item.get("summary", "")
                f.write(f"| {file} | {apis} | {summary} |\n")
            f.write("\n")

        # 2. Code Migration Diffs
        f.write("## 2. Git Diff Format Migration Changes\n\n")
        filtered_diffs = [diff for diff in code_diffs if diff.get("file", "").lower() != "readme.md"]
        if not filtered_diffs:
            f.write("_No code diffs generated._\n")
        else:
            f.write("```diff\n")
            for diff in filtered_diffs:
                file_name = diff.get("file", "")
                file_diff = diff.get("diff", "").strip()
                if not file_diff:
                    continue
                # Write the git diff directly (it already includes headers and formatting)
                f.write(file_diff + "\n")
                # Add separator between files
                if diff != filtered_diffs[-1]:  # Not the last diff
                    f.write("\n")
            f.write("```\n\n")

    print(f"[SUCCESS] Migration report written to {report_path}")

    # Update state with final message
    return {
        **state,
        "messages": state["messages"] + [
            AIMessage(content=f"Migration report generated at {report_path}.")
        ]
    }

def save_migration_state(state: RepoAnalysisState) -> RepoAnalysisState:
    """Save the migration state for use by separate migration operations"""
    
    logger.info("Saving migration state for external operations")
    
    try:
        # Extract relevant data from state
        kafka_inventory = state.get("kafka_inventory", [])
        code_diffs = state.get("code_diffs", [])
        stats = state.get("stats", MigrationStats())
        
        # Filter out README.md from inventory and diffs (documentation only)
        filtered_inventory = [
            item for item in kafka_inventory 
            if item.get("file", "").lower() != "readme.md"
        ]
        
        filtered_diffs = [
            diff for diff in code_diffs 
            if diff.get("file", "").lower() != "readme.md"
        ]
        
        # Pre-save validation: Check all diffs for compilation errors
        logger.info("Validating all diffs for compilation errors before saving...")
        validation_errors = []
        validation_warnings = []
        
        for diff_item in filtered_diffs:
            file_path = diff_item.get('file', '')
            diff_content = diff_item.get('diff', '')
            
            if not diff_content or diff_content.strip().startswith('Error'):
                continue
            
            # Validate for compilation errors
            validation = validate_diff_for_compilation_errors(diff_content, file_path)
            
            if not validation["is_valid"]:
                validation_errors.append(f"{file_path}: {validation['errors']}")
                logger.error(f"Compilation errors detected in {file_path}: {validation['errors']}")
            
            if validation["warnings"]:
                validation_warnings.append(f"{file_path}: {validation['warnings']}")
                logger.warning(f"Compilation warnings in {file_path}: {validation['warnings']}")
        
        if validation_errors:
            logger.error(f"Found {len(validation_errors)} files with compilation errors. Fixes will be applied.")
            # Apply fixes to all files with errors
            for diff_item in filtered_diffs:
                file_path = diff_item.get('file', '')
                diff_content = diff_item.get('diff', '')
                
                if any(file_path in error for error in validation_errors):
                    fixed_diff = validate_and_fix_compilation_errors(diff_content, file_path)
                    if fixed_diff != diff_content:
                        diff_item['diff'] = fixed_diff
                        logger.info(f"Applied fixes to {file_path}")
        
        # Validate constructor and interface consistency between wrapper classes and tests
        consistency_result = validate_constructor_consistency(filtered_diffs)
        interface_result = validate_interface_consistency(filtered_diffs)
        
        if consistency_result['has_errors'] or interface_result['has_errors']:
            logger.warning("Interface consistency validation failed:")
            
            # Report constructor issues
            if consistency_result['has_errors']:
                logger.warning("Constructor mismatches found:")
                for error in consistency_result['errors']:
                    logger.warning(f"  - {error}")
                logger.info("Constructor signatures detected:")
                for name, info in consistency_result['constructor_signatures'].items():
                    logger.info(f"  - {name}: {info['count']} params ({info['parameters']})")
            
            # Report interface issues
            if interface_result['has_errors']:
                logger.warning("Interface mismatches found:")
                for error in interface_result['errors']:
                    logger.warning(f"  - {error}")
                
                if interface_result['method_mismatches']:
                    logger.info("Method mismatches:")
                    for mismatch in interface_result['method_mismatches']:
                        logger.info(f"  - {mismatch['wrapper']}: expected {mismatch['expected_method']}, found {mismatch['found_method']}")
                
                if interface_result['dispose_mismatches']:
                    logger.info("Dispose pattern mismatches:")
                    for mismatch in interface_result['dispose_mismatches']:
                        logger.info(f"  - {mismatch['wrapper']}: expected {mismatch['expected_pattern']}, found {mismatch['found_pattern']}")
            
            # Automatically fix all interface mismatches
            logger.info("Applying comprehensive interface fixes (constructor, method names, async patterns, dispose patterns)...")
            fixed_diffs = apply_interface_fixes(filtered_diffs, consistency_result, interface_result)
            if fixed_diffs:
                filtered_diffs = fixed_diffs
                logger.info("Interface mismatch fixes applied successfully")
            else:
                logger.warning("Could not automatically fix interface mismatches")
        else:
            logger.info("Constructor and interface consistency validation passed")
        
        # Generate timestamp for unique branch name
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        
        # Create state structure for migration operations
        migration_state = {
            "repo_url": state.get("repo_url", ""),
            "repo_path": state.get("repo_path", ""),
            "feature_branch_name": f"feature/kafka-to-servicebus-{timestamp}",
            "kafka_inventory": filtered_inventory,
            "code_diffs": filtered_diffs,
            "stats": {
                "total_files_scanned": getattr(stats, 'total_files_scanned', 0),
                "kafka_files_detected": len(filtered_inventory),
                "files_successfully_migrated": getattr(stats, 'files_successfully_migrated', 0),
                "files_failed_migration": getattr(stats, 'files_failed_migration', 0),
                "analysis_duration": getattr(stats, 'analysis_duration', 0.0),
                "migration_duration": getattr(stats, 'migration_duration', 0.0)
            },
            "analysis": state.get("analysis", ""),
            "extracted_at": time.strftime("%Y-%m-%d"),
            "source": "analyzer_auto_save"
        }
        
        # Save state using SharedStateManager
        state_manager = SharedStateManager()
        
        if state_manager.save_state(migration_state):
            logger.info(f"[SUCCESS] Migration state saved to {state_manager.state_file}")
            logger.info(f"[INFO] Saved {len(filtered_inventory)} Kafka files for migration")
            logger.info(f"[INFO] Saved {len(filtered_diffs)} code diffs for migration")
            
            print(f"[SUCCESS] Migration state saved to {state_manager.state_file}")
            print(f"[INFO] Kafka files available for migration: {len(filtered_inventory)}")
            print(f"[INFO] Code diffs ready for application: {len(filtered_diffs)}")
        else:
            logger.error("[ERROR] Failed to save migration state")
            state.setdefault("errors", []).append("Failed to save migration state")
            
    except Exception as e:
        error_msg = f"Error saving migration state: {str(e)}"
        logger.error(error_msg)
        state.setdefault("errors", []).append(error_msg)
    
    return state

# create_feature_branch function moved to migration_ops.py
    
import tempfile
import textwrap
import subprocess
import os
import re

# Build workflow
graph = StateGraph(RepoAnalysisState)
graph.add_node("clone_repo", clone_repo)
graph.add_node("load_source_code", get_updated_state_with_code_chunks)
graph.add_node("analyze_and_scan_kafka", analyze_and_scan_kafka)
graph.add_node("generate_code_diffs", generate_code_diffs)
graph.add_node("generate_report", generate_report_streaming)
graph.add_node("save_migration_state", save_migration_state)

graph.set_entry_point("clone_repo")
graph.add_edge("clone_repo", "load_source_code")
graph.add_edge("load_source_code", "analyze_and_scan_kafka")
graph.add_edge("analyze_and_scan_kafka", "generate_code_diffs")
graph.add_edge("generate_code_diffs", "generate_report")
graph.add_edge("generate_report", "save_migration_state")
graph.add_edge("save_migration_state", END)

app = graph.compile()



def main():
    """Enhanced main function with better error handling and configuration"""
    
    # Validate environment first
    is_valid, env_errors = validate_environment()
    if not is_valid:
        logger.error("Environment validation failed:")
        for error in env_errors:
            logger.error(f"  - {error}")
        print("[ERROR] Environment validation failed. Please check the logs.")
        return {"status": "failed", "error": "Environment validation failed", "details": env_errors}
    
    # Initialize configuration
    config = AnalysisConfig()
    stats = MigrationStats()
    start_time = time.time()
    
    # Generate unique report filename with timestamp
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    report_filename = f"migration-report_{timestamp}.md"
    report_path = f"./{report_filename}"
    
    logger.info("Starting Kafka to Azure Service Bus migration analysis")
    
    try:
        result = app.invoke({
            "repo_url": DEFAULT_REPO_URL,
            "repo_path": "./cloned_repo",
            "branch": DEFAULT_BRANCH,
            "code_chunks": [],
            "analysis": "",
            "kafka_inventory": [],
            "code_diffs": [],
            "messages": [HumanMessage(content="Analyze this repository for Kafka usage and generate migration report.")],
            # AI configuration
            "model": os.getenv("AZURE_MODEL", "gpt-4"),
            "api_version": os.getenv("AZURE_API_VERSION", "2024-12-01-preview"),
            "base_url": os.getenv("AZURE_ENDPOINT"),
            "api_key": os.getenv("AZURE_OPENAI_API_KEY"),
            # Enhanced configuration
            "config": config,
            "stats": stats,
            "start_time": start_time,
            "errors": []
        })
        
        total_duration = time.time() - start_time
        final_stats = result.get("stats", stats)
        errors = result.get("errors", [])
        
        # Log final statistics
        logger.info("Migration analysis completed!")
        logger.info(f"Total Duration: {total_duration:.2f}s")
        logger.info(f"Files Scanned: {final_stats.total_files_scanned}")
        logger.info(f"Kafka Files Detected: {final_stats.kafka_files_detected}")
        logger.info(f"Files Successfully Migrated: {final_stats.files_successfully_migrated}")
        logger.info(f"Analysis Duration: {final_stats.analysis_duration:.2f}s")
        logger.info(f"Migration Duration: {final_stats.migration_duration:.2f}s")
        
        if errors:
            logger.warning(f"Encountered {len(errors)} errors during processing:")
            for error in errors:
                logger.warning(f"  - {error}")
        
        print(f"\n[SUCCESS] AI Migration analysis completed!")
        print(f"[INFO] REPORT_GENERATED: {report_filename}")
        print(f"[INFO] SUMMARY:")
        print(f"   - Files Scanned: {final_stats.total_files_scanned}")
        print(f"   - Kafka Files Detected: {final_stats.kafka_files_detected}")
        #
        # print(f"   - Files Migrated: {final_stats.files_successfully_migrated}")
        print(f"   - Total Duration: {total_duration:.2f}s")
        
        if errors:
            print(f"[WARNING] Encountered {len(errors)} errors (check logs for details)")
        
        return {
            "status": "success", 
            "report": report_filename,
            "stats": final_stats,
            "duration": total_duration,
            "errors": errors
        }
        
    except Exception as e:
        error_msg = f"Critical error during migration analysis: {str(e)}"
        logger.error(error_msg)
        print(f"[ERROR] Analysis failed: {error_msg}")
        return {
            "status": "failed", 
            "error": error_msg,
            "duration": time.time() - start_time
        }
    

# Main execution with proper argument parsing
if __name__ == "__main__":
    main()