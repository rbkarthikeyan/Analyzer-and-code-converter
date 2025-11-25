import json
import os
import time
from typing import Dict, Any, List
from dataclasses import dataclass, asdict
from pathlib import Path

@dataclass 
class MigrationStats:
    """Statistics tracking for migration process"""
    total_files_scanned: int = 0
    kafka_files_detected: int = 0
    files_successfully_migrated: int = 0
    files_failed_migration: int = 0
    analysis_duration: float = 0.0
    migration_duration: float = 0.0

class SharedStateManager:
    """Manages shared state between separate Python scripts"""
    
    def __init__(self, state_file: str = "migration_state.json"):
        self.state_file = state_file
        
    def save_state(self, state: Dict[str, Any]) -> bool:
        """Save the current state to a JSON file"""
        try:
            # Convert MigrationStats dataclass to dict if present
            if "stats" in state and hasattr(state["stats"], "__dict__"):
                state["stats"] = asdict(state["stats"])
            
            # Convert any non-serializable objects
            serializable_state = self._make_serializable(state)
            
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(serializable_state, f, indent=2, default=str)
            
            print(f"[SUCCESS] State saved to {self.state_file}")
            return True
            
        except Exception as e:
            print(f"[ERROR] Failed to save state: {e}")
            return False
    
    def load_state(self) -> Dict[str, Any]:
        """Load state from JSON file"""
        try:
            if not os.path.exists(self.state_file):
                print(f"[WARNING] State file {self.state_file} does not exist")
                return {}
            
            with open(self.state_file, "r", encoding="utf-8") as f:
                state = json.load(f)
            
            # Convert stats dict back to MigrationStats if present
            if "stats" in state and isinstance(state["stats"], dict):
                state["stats"] = MigrationStats(**state["stats"])
            
            print(f"[SUCCESS] State loaded from {self.state_file}")
            return state
            
        except Exception as e:
            print(f"[ERROR] Failed to load state: {e}")
            return {}
    
    def update_state(self, updates: Dict[str, Any]) -> bool:
        """Update existing state with new values"""
        current_state = self.load_state()
        current_state.update(updates)
        return self.save_state(current_state)
    
    def get_state_value(self, key: str, default=None):
        """Get a specific value from the state"""
        state = self.load_state()
        return state.get(key, default)
    
    def state_exists(self) -> bool:
        """Check if state file exists"""
        return os.path.exists(self.state_file)
    
    def _make_serializable(self, obj):
        """Convert objects to JSON-serializable format"""
        if isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(item) for item in obj]
        elif hasattr(obj, "__dict__"):
            return asdict(obj) if hasattr(obj, "__dataclass_fields__") else str(obj)
        else:
            return obj

def validate_required_state(state: Dict[str, Any], required_keys: List[str]) -> bool:
    """Validate that required keys exist in the state"""
    missing_keys = [key for key in required_keys if key not in state]
    if missing_keys:
        print(f"[ERROR] Missing required state keys: {missing_keys}")
        return False
    return True