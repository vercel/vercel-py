#!/usr/bin/env python3
"""
E2E Test Runner for Vercel Python SDK

This script runs end-to-end tests for the Vercel Python SDK,
checking all major workflows and integrations.
"""

import sys
import subprocess
import argparse
from pathlib import Path

from tests.e2e.config import E2ETestConfig

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


class E2ETestRunner:
    """Runner for E2E tests."""

    def __init__(self):
        self.config = E2ETestConfig()
        self.test_results = {}

    def check_environment(self) -> bool:
        """Check if the test environment is properly configured."""
        print("Checking E2E test environment...")
        self.config.print_env_status()

        # Check if at least one service is available
        services_available = [
            self.config.is_blob_enabled(),
            self.config.is_vercel_api_enabled(),
            self.config.is_oidc_enabled(),
        ]

        if not any(services_available):
            print("❌ No services available for testing!")
            print("Please set at least one of the following environment variables:")
            print(f"  - {self.config.BLOB_TOKEN_ENV}")
            print(f"  - {self.config.VERCEL_TOKEN_ENV}")
            print(f"  - {self.config.OIDC_TOKEN_ENV}")
            return False

        print("✅ Environment check passed!")
        return True

    def run_unit_tests(self) -> bool:
        """Run unit tests first."""
        print("\n🧪 Running unit tests...")
        try:
            result = subprocess.run(
                [sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short"],
                capture_output=True,
                text=True,
                timeout=300,
            )

            if result.returncode == 0:
                print("✅ Unit tests passed!")
                return True
            else:
                print("❌ Unit tests failed!")
                print("STDOUT:", result.stdout)
                print("STDERR:", result.stderr)
                return False
        except subprocess.TimeoutExpired:
            print("❌ Unit tests timed out!")
            return False
        except Exception as e:
            print(f"❌ Error running unit tests: {e}")
            return False

    def run_e2e_tests(self, test_pattern: str = None) -> bool:
        """Run E2E tests."""
        print("\n🚀 Running E2E tests...")

        cmd = [sys.executable, "-m", "pytest", "tests/e2e/", "-v", "--tb=short"]

        if test_pattern:
            cmd.extend(["-k", test_pattern])

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)

            if result.returncode == 0:
                print("✅ E2E tests passed!")
                return True
            else:
                print("❌ E2E tests failed!")
                print("STDOUT:", result.stdout)
                print("STDERR:", result.stderr)
                return False
        except subprocess.TimeoutExpired:
            print("❌ E2E tests timed out!")
            return False
        except Exception as e:
            print(f"❌ Error running E2E tests: {e}")
            return False

    def run_integration_tests(self) -> bool:
        """Run integration tests."""
        print("\n🔗 Running integration tests...")

        try:
            result = subprocess.run(
                [sys.executable, "-m", "pytest", "tests/integration/", "-v", "--tb=short"],
                capture_output=True,
                text=True,
                timeout=600,
            )

            if result.returncode == 0:
                print("✅ Integration tests passed!")
                return True
            else:
                print("❌ Integration tests failed!")
                print("STDOUT:", result.stdout)
                print("STDERR:", result.stderr)
                return False
        except subprocess.TimeoutExpired:
            print("❌ Integration tests timed out!")
            return False
        except Exception as e:
            print(f"❌ Error running integration tests: {e}")
            return False

    def run_examples(self) -> bool:
        """Run example scripts as smoke tests."""
        print("\n📚 Running example scripts...")

        examples_dir = Path(__file__).parent / "examples"
        if not examples_dir.exists():
            print("❌ Examples directory not found!")
            return False

        example_files = list(examples_dir.glob("*.py"))
        if not example_files:
            print("❌ No example files found!")
            return False

        success_count = 0
        for example_file in example_files:
            print(f"  Running {example_file.name}...")
            try:
                result = subprocess.run(
                    [sys.executable, str(example_file)], capture_output=True, text=True, timeout=60
                )

                if result.returncode == 0:
                    print(f"  ✅ {example_file.name} passed!")
                    success_count += 1
                else:
                    print(f"  ❌ {example_file.name} failed!")
                    print(f"    STDOUT: {result.stdout}")
                    print(f"    STDERR: {result.stderr}")
            except subprocess.TimeoutExpired:
                print(f"  ❌ {example_file.name} timed out!")
            except Exception as e:
                print(f"  ❌ Error running {example_file.name}: {e}")

        if success_count == len(example_files):
            print("✅ All example scripts passed!")
            return True
        else:
            print(f"❌ {len(example_files) - success_count} example scripts failed!")
            return False

    def run_all_tests(self, test_pattern: str = None) -> bool:
        """Run all tests."""
        print("🧪 Starting comprehensive E2E test suite...")
        print("=" * 60)

        # Check environment
        if not self.check_environment():
            return False

        # Run unit tests
        if not self.run_unit_tests():
            return False

        # Run E2E tests
        if not self.run_e2e_tests(test_pattern):
            return False

        # Run integration tests
        if not self.run_integration_tests():
            return False

        # Run examples
        if not self.run_examples():
            return False

        print("\n" + "=" * 60)
        print("🎉 All tests passed! E2E test suite completed successfully.")
        return True

    def run_specific_tests(self, test_type: str, test_pattern: str = None) -> bool:
        """Run specific type of tests."""
        print(f"🧪 Running {test_type} tests...")

        if test_type == "unit":
            return self.run_unit_tests()
        elif test_type == "e2e":
            return self.run_e2e_tests(test_pattern)
        elif test_type == "integration":
            return self.run_integration_tests()
        elif test_type == "examples":
            return self.run_examples()
        else:
            print(f"❌ Unknown test type: {test_type}")
            return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="E2E Test Runner for Vercel Python SDK")
    parser.add_argument(
        "--test-type",
        choices=["all", "unit", "e2e", "integration", "examples"],
        default="all",
        help="Type of tests to run",
    )
    parser.add_argument("--pattern", help="Test pattern to match (for e2e tests)")
    parser.add_argument(
        "--check-env", action="store_true", help="Only check environment configuration"
    )

    args = parser.parse_args()

    runner = E2ETestRunner()

    if args.check_env:
        success = runner.check_environment()
    elif args.test_type == "all":
        success = runner.run_all_tests(args.pattern)
    else:
        success = runner.run_specific_tests(args.test_type, args.pattern)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
