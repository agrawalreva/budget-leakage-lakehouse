import pytest
from config.config_loader import load_config

def test_config_loading():
    config = load_config()
    
    assert config is not None
    assert "paths" in config
    assert "spark" in config
    assert "leakage_detection" in config
    
    assert "bronze" in config["paths"]
    assert "silver" in config["paths"]
    assert "gold" in config["paths"]

def test_config_spark_settings():
    config = load_config()
    
    assert "app_name" in config["spark"]
    assert "executor_memory" in config["spark"]
    assert "adaptive_enabled" in config["spark"]

def test_config_leakage_detection():
    config = load_config()
    
    leak_config = config["leakage_detection"]
    assert "duplicate_window_days" in leak_config
    assert "sub_threshold_amount" in leak_config
    assert leak_config["duplicate_window_days"] > 0
    assert leak_config["sub_threshold_amount"] > 0

