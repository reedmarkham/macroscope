"""
Configuration Management Library

Provides centralized configuration loading and management for the 
electron microscopy data ingestion pipeline.
"""

import os
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Union
from dataclasses import dataclass


@dataclass
class SourceConfig:
    """Configuration for a specific data source."""
    name: str
    enabled: bool
    output_dir: str
    base_urls: Dict[str, str]
    processing: Dict[str, Any]
    metadata_mapping: Dict[str, Any]


class ConfigManager:
    """
    Centralized configuration management with environment variable support.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to configuration file. If None, uses default location.
        """
        self.logger = logging.getLogger(__name__)
        
        if config_path is None:
            # Default to config.yaml in config directory relative to this file
            current_dir = Path(__file__).parent
            config_path = current_dir.parent / "config" / "config.yaml"
        
        self.config_path = Path(config_path)
        self._config = None
        self._load_config()
    
    def _load_config(self) -> None:
        """Load configuration from YAML file with environment variable substitution."""
        try:
            with open(self.config_path, 'r') as f:
                config_content = f.read()
            
            # Substitute environment variables
            config_content = self._substitute_env_vars(config_content)
            
            # Parse YAML
            self._config = yaml.safe_load(config_content)
            
            # Validate configuration
            self._validate_config()
            
            self.logger.info(f"Loaded configuration from {self.config_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to load configuration from {self.config_path}: {e}")
            raise
    
    def _substitute_env_vars(self, content: str) -> str:
        """
        Substitute environment variables in configuration content.
        
        Supports ${VAR_NAME} and ${VAR_NAME:default_value} syntax.
        """
        import re
        
        def replace_env_var(match):
            var_expr = match.group(1)
            if ':' in var_expr:
                var_name, default_value = var_expr.split(':', 1)
                return os.environ.get(var_name, default_value)
            else:
                return os.environ.get(var_expr, match.group(0))
        
        # Pattern matches ${VAR_NAME} or ${VAR_NAME:default}
        pattern = r'\$\{([^}]+)\}'
        return re.sub(pattern, replace_env_var, content)
    
    def _validate_config(self) -> None:
        """Validate loaded configuration structure."""
        required_sections = ['global', 'sources']
        
        for section in required_sections:
            if section not in self._config:
                raise ValueError(f"Missing required configuration section: {section}")
        
        # Validate sources
        for source_name, source_config in self._config['sources'].items():
            required_fields = ['name', 'enabled', 'output_dir', 'base_urls']
            for field in required_fields:
                if field not in source_config:
                    raise ValueError(f"Missing required field '{field}' in source '{source_name}'")
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        
        Args:
            key_path: Dot-separated path to configuration value (e.g., 'global.processing.max_workers')
            default: Default value if key not found
        
        Returns:
            Configuration value or default
        """
        keys = key_path.split('.')
        value = self._config
        
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_global_config(self) -> Dict[str, Any]:
        """Get global configuration section."""
        return self._config.get('global', {})
    
    def get_source_config(self, source_name: str) -> Optional[SourceConfig]:
        """
        Get configuration for a specific data source.
        
        Args:
            source_name: Name of the data source (ebi, epfl, flyem, idr, openorganelle)
        
        Returns:
            SourceConfig object or None if source not found
        """
        source_config = self._config.get('sources', {}).get(source_name)
        if not source_config:
            return None
        
        return SourceConfig(
            name=source_config['name'],
            enabled=source_config['enabled'],
            output_dir=source_config['output_dir'],
            base_urls=source_config['base_urls'],
            processing=source_config.get('processing', {}),
            metadata_mapping=source_config.get('metadata_mapping', {})
        )
    
    def get_enabled_sources(self) -> Dict[str, SourceConfig]:
        """Get all enabled data source configurations."""
        enabled_sources = {}
        
        for source_name in self._config.get('sources', {}):
            source_config = self.get_source_config(source_name)
            if source_config and source_config.enabled:
                enabled_sources[source_name] = source_config
        
        return enabled_sources
    
    def get_consolidation_config(self) -> Dict[str, Any]:
        """Get consolidation tool configuration."""
        return self._config.get('consolidation', {})
    
    def get_docker_config(self) -> Dict[str, Any]:
        """Get Docker configuration."""
        return self._config.get('docker', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration."""
        return self.get('global.logging', {})
    
    def get_processing_config(self) -> Dict[str, Any]:
        """Get global processing configuration."""
        return self.get('global.processing', {})
    
    def get_metadata_config(self) -> Dict[str, Any]:
        """Get metadata management configuration."""
        return self.get('global.metadata', {})
    
    def set(self, key_path: str, value: Any) -> None:
        """
        Set configuration value using dot notation.
        
        Args:
            key_path: Dot-separated path to configuration value
            value: Value to set
        """
        keys = key_path.split('.')
        config = self._config
        
        # Navigate to parent of target key
        for key in keys[:-1]:
            if key not in config:
                config[key] = {}
            config = config[key]
        
        # Set the value
        config[keys[-1]] = value
    
    def update_source_config(self, source_name: str, updates: Dict[str, Any]) -> None:
        """
        Update configuration for a specific source.
        
        Args:
            source_name: Name of the data source
            updates: Dictionary of updates to apply
        """
        if 'sources' not in self._config:
            self._config['sources'] = {}
        
        if source_name not in self._config['sources']:
            self._config['sources'][source_name] = {}
        
        self._config['sources'][source_name].update(updates)
    
    def save_config(self, output_path: Optional[str] = None) -> None:
        """
        Save current configuration to file.
        
        Args:
            output_path: Path to save configuration. If None, overwrites original file.
        """
        if output_path is None:
            output_path = self.config_path
        
        try:
            with open(output_path, 'w') as f:
                yaml.dump(self._config, f, default_flow_style=False, indent=2, sort_keys=True)
            
            self.logger.info(f"Saved configuration to {output_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save configuration to {output_path}: {e}")
            raise
    
    def reload_config(self) -> None:
        """Reload configuration from file."""
        self._load_config()
    
    def get_data_dir(self, source_name: str, create: bool = True) -> Path:
        """
        Get data directory for a specific source.
        
        Args:
            source_name: Name of the data source
            create: Whether to create directory if it doesn't exist
        
        Returns:
            Path to data directory
        """
        source_config = self.get_source_config(source_name)
        if not source_config:
            raise ValueError(f"Unknown source: {source_name}")
        
        data_dir = Path(source_config.output_dir)
        
        if create and not data_dir.exists():
            data_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Created data directory: {data_dir}")
        
        return data_dir
    
    def get_logs_dir(self, create: bool = True) -> Path:
        """
        Get logs directory.
        
        Args:
            create: Whether to create directory if it doesn't exist
        
        Returns:
            Path to logs directory
        """
        logs_dir = Path(self.get('global.logs_root', './logs'))
        
        if create and not logs_dir.exists():
            logs_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Created logs directory: {logs_dir}")
        
        return logs_dir
    
    def setup_logging(self) -> None:
        """Configure logging based on configuration settings."""
        logging_config = self.get_logging_config()
        
        # Configure root logger
        logging.basicConfig(
            level=logging_config.get('level', 'INFO'),
            format=logging_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            handlers=[
                logging.StreamHandler(),  # Console output
                logging.FileHandler(
                    self.get_logs_dir() / 'pipeline.log',
                    maxBytes=logging_config.get('max_size_mb', 10) * 1024 * 1024,
                    backupCount=logging_config.get('backup_count', 5)
                )
            ]
        )
    
    def get_schema_path(self) -> Path:
        """Get path to metadata schema file."""
        schema_path = self.get('global.metadata.schema_path', './schemas/metadata_schema.json')
        return Path(schema_path)
    
    def is_development_mode(self) -> bool:
        """Check if development mode is enabled."""
        return self.get('development.debug_mode', False)


# Global configuration instance
_config_manager = None


def get_config_manager(config_path: Optional[str] = None) -> ConfigManager:
    """
    Get global configuration manager instance.
    
    Args:
        config_path: Path to configuration file (only used on first call)
    
    Returns:
        ConfigManager instance
    """
    global _config_manager
    
    if _config_manager is None:
        _config_manager = ConfigManager(config_path)
    
    return _config_manager


def get_config(key_path: str, default: Any = None) -> Any:
    """
    Convenience function to get configuration value.
    
    Args:
        key_path: Dot-separated path to configuration value
        default: Default value if key not found
    
    Returns:
        Configuration value or default
    """
    return get_config_manager().get(key_path, default)