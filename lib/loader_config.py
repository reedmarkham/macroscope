"""
Loader Configuration Classes

Provides parameterized configuration for all data source loaders,
enabling testing and flexible deployment scenarios.
"""


from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
import os


@dataclass
class BaseLoaderConfig:
    """Base configuration class for all loaders."""
    output_dir: str
    max_workers: int = 4
    timeout_seconds: int = 10800
    verbose: bool = False
    dry_run: bool = False


@dataclass
class EBIConfig(BaseLoaderConfig):
    """Configuration for EBI EMPIAR loader."""
    entry_id: str = "11759"
    ftp_server: str = "ftp.ebi.ac.uk"
    api_base_url: str = "https://www.ebi.ac.uk/empiar/api/entry"
    download_subdir: str = "downloads"
    output_dir: str = "empiar_volumes"
    
    @property
    def api_url(self) -> str:
        return f"{self.api_base_url}/{self.entry_id}/"
    
    def get_ftp_path(self, filename: str) -> str:
        return f"/empiar/world_availability/{self.entry_id}/data/{filename}"


@dataclass
class EPFLConfig(BaseLoaderConfig):
    """Configuration for EPFL CVLab loader."""
    download_url: str = "https://documents.epfl.ch/groups/c/cv/cvlab-unit/www/data/%20ElectronMicroscopy_Hippocampus/volumedata.tif"
    source_id: str = "EPFL-CA1-HIPPOCAMPUS"
    description: str = "5x5x5µm section from CA1 hippocampus region"
    voxel_size_nm: List[float] = field(default_factory=lambda: [5.0, 5.0, 5.0])
    chunk_size_mb: int = 8
    output_dir: str = "epfl_em_data"


@dataclass
class FlyEMConfig(BaseLoaderConfig):
    """Configuration for FlyEM/DVID loader."""
    dvid_server: str = "http://hemibrain-dvid.janelia.org"
    uuid: str = "a89eb3af216a46cdba81204d8f954786"
    instance: str = "grayscale"
    crop_size: Tuple[int, int, int] = (1000, 1000, 1000)
    random_seed: Optional[int] = None
    output_dir: str = "dvid_crops"
    
    def __post_init__(self):
        # Override instance from environment if available
        env_instance = os.environ.get("GRAYSCALE_INSTANCE")
        if env_instance:
            self.instance = env_instance


@dataclass
class IDRConfig(BaseLoaderConfig):
    """Configuration for IDR loader."""
    image_ids: List[int] = field(default_factory=lambda: [9846137])
    dataset_id: str = "idr0086"
    ftp_host: str = "ftp.ebi.ac.uk"
    ftp_root_path: str = "/pub/databases/IDR"
    api_base_url: str = "https://idr.openmicroscopy.org/api/v0/m/"
    output_dir: str = "idr_volumes"
    
    # Hardcoded path mappings - could be moved to config file
    path_mappings: Dict[str, str] = field(default_factory=lambda: {
        "idr0086": "idr0086-miron-micrographs/20200610-ftp/experimentD/Miron_FIB-SEM/Miron_FIB-SEM_processed"
    })
    
    def __post_init__(self):
        # Override output dir from environment if available
        env_output_dir = os.environ.get("IDR_OUTPUT_DIR")
        if env_output_dir:
            self.output_dir = env_output_dir
    
    def get_ftp_path(self, dataset_id: str) -> str:
        return f"{self.ftp_root_path}/{self.path_mappings.get(dataset_id, dataset_id)}"


@dataclass
class OpenOrganelleConfig(BaseLoaderConfig):
    """Configuration for OpenOrganelle loader."""
    s3_uri: str = "s3://janelia-cosem-datasets/jrc_mus-nacc-2/jrc_mus-nacc-2.zarr"
    zarr_path: str = "recon-2/em"
    dataset_id: str = "jrc_mus-nacc-2"
    voxel_size: Dict[str, float] = field(default_factory=lambda: {"x": 4.0, "y": 4.0, "z": 2.96})
    dimensions_nm: Dict[str, float] = field(default_factory=lambda: {"x": 10384, "y": 10080, "z": 1669.44})
    s3_anonymous: bool = True
    output_dir: str = "zarr_volume"
    max_workers: int = 4
    
    @property
    def bucket_name(self) -> str:
        return self.s3_uri.split('/')[2]
    
    @property
    def s3_key(self) -> str:
        return '/'.join(self.s3_uri.split('/')[3:])


@dataclass
class ProcessingResult:
    """Standard result object returned by all loaders."""
    success: bool
    source: str
    source_id: str
    files_processed: List[str] = field(default_factory=list)
    metadata_paths: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    processing_time_seconds: Optional[float] = None
    total_size_bytes: Optional[int] = None
    
    def add_error(self, error: str) -> None:
        """Add an error message to the result."""
        self.errors.append(error)
        self.success = False
    
    def add_processed_file(self, file_path: str, metadata_path: Optional[str] = None) -> None:
        """Add a successfully processed file to the result."""
        self.files_processed.append(file_path)
        if metadata_path:
            self.metadata_paths.append(metadata_path)


def create_config_from_dict(loader_type: str, config_dict: Dict[str, Any]) -> BaseLoaderConfig:
    """
    Create a loader configuration from a dictionary.
    
    Args:
        loader_type: Type of loader (ebi, epfl, flyem, idr, openorganelle)
        config_dict: Configuration parameters as dictionary
    
    Returns:
        Appropriate configuration object
    
    Raises:
        ValueError: If loader_type is not recognized
    """
    config_classes = {
        'ebi': EBIConfig,
        'epfl': EPFLConfig,
        'flyem': FlyEMConfig,
        'idr': IDRConfig,
        'openorganelle': OpenOrganelleConfig,
    }
    
    if loader_type not in config_classes:
        raise ValueError(f"Unknown loader type: {loader_type}. Valid types: {list(config_classes.keys())}")
    
    config_class = config_classes[loader_type]
    
    # Filter config_dict to only include fields that exist in the dataclass
    import inspect
    valid_fields = {f.name for f in config_class.__dataclass_fields__.values()}
    filtered_dict = {k: v for k, v in config_dict.items() if k in valid_fields}
    
    return config_class(**filtered_dict)


def load_test_configs() -> Dict[str, BaseLoaderConfig]:
    """
    Load default test configurations for all loaders.
    
    Returns:
        Dictionary mapping loader names to their test configurations
    """
    return {
        'ebi': EBIConfig(
            entry_id="11759",
            output_dir="test_data/ebi",
            max_workers=2,
            timeout_seconds=10800
        ),
        'epfl': EPFLConfig(
            output_dir="test_data/epfl",
            max_workers=2,
            timeout_seconds=10800
        ),
        'flyem': FlyEMConfig(
            crop_size=(100, 100, 100),  # Smaller for testing
            output_dir="test_data/flyem",
            max_workers=2,
            timeout_seconds=10800,
            random_seed=42  # Reproducible tests
        ),
        'idr': IDRConfig(
            image_ids=[9846137],
            output_dir="test_data/idr",
            max_workers=2,
            timeout_seconds=10800
        ),
        'openorganelle': OpenOrganelleConfig(
            output_dir="test_data/openorganelle",
            max_workers=2,
            timeout_seconds=10800
        )
    }


def create_minimal_test_config(loader_type: str, **overrides) -> BaseLoaderConfig:
    """
    Create a minimal configuration suitable for fast testing.
    
    Args:
        loader_type: Type of loader
        **overrides: Configuration overrides
    
    Returns:
        Minimal test configuration
    """
    test_configs = load_test_configs()
    base_config = test_configs[loader_type]
    
    # Apply overrides
    for key, value in overrides.items():
        if hasattr(base_config, key):
            setattr(base_config, key, value)
    
    return base_config