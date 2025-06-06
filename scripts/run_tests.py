"""
Test runner for the electron microscopy ingestion pipeline.

This script provides a convenient interface for running different types of tests
with various configurations and reporting options.
"""

import argparse
import sys
import subprocess
import os
from pathlib import Path
from typing import List, Optional
import tempfile
import json

# Change to project root directory (parent of scripts directory)
script_dir = Path(__file__).parent
project_root = script_dir.parent
os.chdir(project_root)


def run_command(cmd: List[str], cwd: Optional[Path] = None) -> int:
    """
    Run a command and return the exit code.
    
    Args:
        cmd: Command and arguments to run
        cwd: Working directory for the command
    
    Returns:
        Exit code from the command
    """
    print(f"Running: {' '.join(cmd)}")
    if cwd:
        print(f"Working directory: {cwd}")
    
    try:
        result = subprocess.run(cmd, cwd=cwd, check=False)
        return result.returncode
    except FileNotFoundError:
        print(f"Error: Command not found: {cmd[0]}")
        return 1


def setup_test_environment() -> Path:
    """
    Set up the test environment and return the test directory.
    
    Returns:
        Path to the test directory
    """
    project_root = Path(__file__).parent
    
    # Add project root to Python path
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    
    # Create test output directory
    test_output_dir = project_root / "test_output"
    test_output_dir.mkdir(exist_ok=True)
    
    # Set environment variables for testing
    os.environ['EM_INGEST_TEST_MODE'] = 'true'
    os.environ['EM_INGEST_TEST_DIR'] = str(test_output_dir)
    
    return test_output_dir


def run_unit_tests(args) -> int:
    """Run unit tests."""
    cmd = ['python', '-m', 'pytest']
    
    # Add test selection
    if args.loader:
        cmd.extend(['-k', f'test_unit_{args.loader}'])
    else:
        cmd.extend(['tests/', '-k', 'test_unit'])
    
    # Add verbosity
    if args.verbose:
        cmd.append('-v')
    if args.very_verbose:
        cmd.append('-vv')
    
    # Add coverage if requested
    if args.coverage:
        cmd.extend(['--cov=lib', '--cov=app', '--cov-report=html', '--cov-report=term'])
    
    # Add markers
    if args.fast:
        cmd.extend(['-m', 'not slow'])
    
    # Add output options
    if args.junit_xml:
        cmd.extend(['--junit-xml', args.junit_xml])
    
    return run_command(cmd)


def run_integration_tests(args) -> int:
    """Run integration tests."""
    cmd = ['python', '-m', 'pytest']
    
    # Add test selection
    if args.loader:
        cmd.extend(['-k', f'integration and {args.loader}'])
    else:
        cmd.extend(['tests/test_integration.py'])
    
    # Add verbosity
    if args.verbose:
        cmd.append('-v')
    if args.very_verbose:
        cmd.append('-vv')
    
    # Add markers
    cmd.extend(['-m', 'integration'])
    if args.fast:
        cmd.extend(['and not slow'])
    
    # Add network tests if requested
    if not args.no_network:
        print("Including network-based integration tests")
    else:
        cmd.extend(['-m', 'integration and not network'])
    
    return run_command(cmd)


def run_loader_test(loader_name: str, args) -> int:
    """
    Run tests for a specific loader.
    
    Args:
        loader_name: Name of the loader to test
        args: Command line arguments
    
    Returns:
        Exit code
    """
    if loader_name not in ['ebi', 'epfl', 'flyem', 'idr', 'openorganelle']:
        print(f"Error: Unknown loader '{loader_name}'")
        print("Valid loaders: ebi, epfl, flyem, idr, openorganelle")
        return 1
    
    cmd = ['python', '-m', 'pytest']
    
    # Add specific loader tests
    cmd.extend(['-k', f'{loader_name}'])
    
    # Add verbosity
    if args.verbose:
        cmd.append('-v')
    if args.very_verbose:
        cmd.append('-vv')
    
    # Add markers
    if args.fast:
        cmd.extend(['-m', 'not slow'])
    
    if args.unit_only:
        cmd.extend(['-m', 'not integration'])
    
    return run_command(cmd)


def run_metadata_tests(args) -> int:
    """Test the metadata manager library."""
    cmd = ['python', '-m', 'pytest']
    
    # Add specific metadata manager tests
    cmd.extend(['-k', 'metadata_manager'])
    
    # Add verbosity
    if args.verbose:
        cmd.append('-v')
    if args.very_verbose:
        cmd.append('-vv')
    
    # Add coverage for metadata manager
    if args.coverage:
        cmd.extend(['--cov=lib.metadata_manager', '--cov-report=html', '--cov-report=term'])
    
    # Add markers
    if args.fast:
        cmd.extend(['-m', 'not slow'])
    
    return run_command(cmd)


def run_consolidation_test(args) -> int:
    """Test the metadata consolidation tool."""
    test_output_dir = setup_test_environment()
    
    # Create test metadata files
    test_sources = ['ebi', 'epfl', 'flyem']
    
    for source in test_sources:
        source_dir = test_output_dir / source
        source_dir.mkdir(exist_ok=True)
        
        # Create test metadata
        metadata = {
            "id": f"test-{source}-001",
            "source": source,
            "source_id": f"TEST-{source.upper()}-001", 
            "status": "complete",
            "created_at": "2024-01-01T12:00:00Z",
            "updated_at": "2024-01-01T12:30:00Z",
            "metadata": {
                "core": {
                    "description": f"Test {source} dataset for consolidation",
                    "volume_shape": [100, 100, 100],
                    "data_type": "uint8"
                }
            }
        }
        
        metadata_file = source_dir / f"metadata_{source}_test.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
    
    # Run consolidation
    consolidate_script = Path(__file__).parent / "app" / "consolidate" / "main.py"
    
    cmd = [
        'python', str(consolidate_script),
        '--config', str(Path(__file__).parent / "config" / "config.yaml")
    ]
    
    if args.verbose:
        print(f"Testing consolidation with test data in {test_output_dir}")
    
    return run_command(cmd, cwd=Path(__file__).parent)


def run_performance_tests(args) -> int:
    """Run performance benchmark tests."""
    cmd = ['python', '-m', 'pytest']
    
    # Add performance tests
    cmd.extend(['-k', 'performance'])
    
    # Add verbosity
    if args.verbose:
        cmd.append('-v')
    
    # Add benchmark plugin if available
    try:
        import pytest_benchmark
        cmd.extend(['--benchmark-only'])
        if args.benchmark_compare:
            cmd.extend(['--benchmark-compare', args.benchmark_compare])
    except ImportError:
        print("Warning: pytest-benchmark not installed, running basic performance tests")
    
    return run_command(cmd)


def generate_test_report(args) -> int:
    """Generate a comprehensive test report."""
    test_output_dir = setup_test_environment()
    report_dir = test_output_dir / "reports"
    report_dir.mkdir(exist_ok=True)
    
    print("Generating comprehensive test report...")
    
    # Run all tests with reporting
    cmd = [
        'python', '-m', 'pytest',
        'tests/',
        '--html=' + str(report_dir / "report.html"),
        '--self-contained-html',
        '--junit-xml=' + str(report_dir / "junit.xml"),
        '--cov=lib',
        '--cov=app', 
        '--cov-report=html:' + str(report_dir / "coverage"),
        '--cov-report=xml:' + str(report_dir / "coverage.xml"),
        '-v'
    ]
    
    if args.fast:
        cmd.extend(['-m', 'not slow'])
    
    exit_code = run_command(cmd)
    
    print(f"\nTest report generated in: {report_dir}")
    print(f"HTML report: {report_dir / 'report.html'}")
    print(f"Coverage report: {report_dir / 'coverage' / 'index.html'}")
    
    return exit_code


def main():
    """Main test runner function."""
    parser = argparse.ArgumentParser(
        description="Test runner for electron microscopy ingestion pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s unit                     # Run all unit tests
  %(prog)s unit --loader ebi        # Run EBI unit tests
  %(prog)s integration              # Run integration tests  
  %(prog)s loader ebi               # Run all EBI tests
  %(prog)s metadata                 # Test metadata manager library
  %(prog)s consolidation            # Test consolidation tool
  %(prog)s performance              # Run performance tests
  %(prog)s report                   # Generate full test report
  %(prog)s unit --fast --coverage   # Fast unit tests with coverage
        """
    )
    
    # Global options
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Verbose output')
    parser.add_argument('-vv', '--very-verbose', action='store_true',
                       help='Very verbose output')
    parser.add_argument('--fast', action='store_true',
                       help='Skip slow tests')
    parser.add_argument('--coverage', action='store_true',
                       help='Generate coverage report')
    parser.add_argument('--junit-xml', type=str,
                       help='Generate JUnit XML report')
    
    # Subcommands
    subparsers = parser.add_subparsers(dest='command', help='Test command')
    
    # Unit tests
    unit_parser = subparsers.add_parser('unit', help='Run unit tests')
    unit_parser.add_argument('--loader', choices=['ebi', 'epfl', 'flyem', 'idr', 'openorganelle'],
                            help='Test specific loader only')
    
    # Integration tests  
    integration_parser = subparsers.add_parser('integration', help='Run integration tests')
    integration_parser.add_argument('--loader', choices=['ebi', 'epfl', 'flyem', 'idr', 'openorganelle'],
                                   help='Test specific loader only')
    integration_parser.add_argument('--no-network', action='store_true',
                                   help='Skip network-dependent tests')
    
    # Loader-specific tests
    loader_parser = subparsers.add_parser('loader', help='Run tests for specific loader')
    loader_parser.add_argument('name', choices=['ebi', 'epfl', 'flyem', 'idr', 'openorganelle'],
                              help='Loader name')
    loader_parser.add_argument('--unit-only', action='store_true',
                              help='Run unit tests only')
    
    # Metadata manager tests
    metadata_parser = subparsers.add_parser('metadata', help='Test metadata manager library')
    
    # Consolidation tests
    consolidation_parser = subparsers.add_parser('consolidation', help='Test consolidation tool')
    
    # Performance tests
    performance_parser = subparsers.add_parser('performance', help='Run performance tests')
    performance_parser.add_argument('--benchmark-compare', type=str,
                                   help='Compare with previous benchmark results')
    
    # Test report
    report_parser = subparsers.add_parser('report', help='Generate comprehensive test report')
    
    # All tests
    all_parser = subparsers.add_parser('all', help='Run all tests')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Setup test environment
    setup_test_environment()
    
    # Route to appropriate test runner
    if args.command == 'unit':
        return run_unit_tests(args)
    elif args.command == 'integration':
        return run_integration_tests(args)
    elif args.command == 'loader':
        return run_loader_test(args.name, args)
    elif args.command == 'metadata':
        return run_metadata_tests(args)
    elif args.command == 'consolidation':
        return run_consolidation_test(args)
    elif args.command == 'performance':
        return run_performance_tests(args)
    elif args.command == 'report':
        return generate_test_report(args)
    elif args.command == 'all':
        # Run all test types
        exit_codes = []
        
        print("=== Running Unit Tests ===")
        exit_codes.append(run_unit_tests(args))
        
        print("\n=== Running Integration Tests ===")
        exit_codes.append(run_integration_tests(args))
        
        print("\n=== Testing Metadata Manager ===")
        exit_codes.append(run_metadata_tests(args))
        
        print("\n=== Testing Consolidation ===")  
        exit_codes.append(run_consolidation_test(args))
        
        # Return non-zero if any tests failed
        return max(exit_codes)
    else:
        parser.print_help()
        return 1


if __name__ == '__main__':
    sys.exit(main())