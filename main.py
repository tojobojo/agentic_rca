"""
Main Entrypoint for the Agentic RCA System.
This script orchestrates the entire RCA workflow:
1. Discovery (Databricks API + GitLab)
2. Lineage (Unity Catalog API or Manifest)
3. Parsing (Extract logic from code)
4. Validation (Shadow counts + Anomaly detection)
5. RCA (AI-powered investigation)
6. Performance Analysis (Optional - Query Plan debugging)
7. Reporting (Markdown output)
"""
import argparse
import json
import os
import sys
from datetime import datetime
from typing import Optional, List, Dict

# Ensure package imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import get_config, Config
from discovery_agent import DiscoveryAgent
from pipeline_parser import PipelineParser, ParsedStep
from validation_engine import ValidationEngine
from rca_agent import RCAAgent
from performance_agent import PerformanceAgent, PerformanceContext
from lineage_client import get_step_tables


def run_rca_pipeline(
    job_id: int,
    gitlab_url: str,
    branch: str = "main",
    output_path: Optional[str] = None,
    analyze_performance: bool = False,
    manifest_path: Optional[str] = None,
    spark_session=None
) -> str:
    """
    Execute the full RCA pipeline.
    
    Args:
        job_id: Databricks Job ID
        gitlab_url: GitLab repository URL
        branch: Git branch to analyze
        output_path: Path to save the RCA report (optional)
        analyze_performance: Whether to run performance analysis
        manifest_path: Path to JSON manifest with explicit table mappings
        spark_session: Active Spark session (optional, will create if needed)
    
    Returns:
        The generated RCA report as Markdown string
    """
    print("=" * 60)
    print("       AGENTIC RCA SYSTEM - Production Mode")
    print("=" * 60)
    print(f"Job ID: {job_id}")
    print(f"GitLab: {gitlab_url}")
    print(f"Branch: {branch}")
    print(f"Performance Analysis: {'Enabled' if analyze_performance else 'Disabled'}")
    print(f"Manifest: {manifest_path or 'None (using Lineage API)'}")
    print("=" * 60)
    
    # Phase 1: Discovery
    print("\n[Phase 1] DISCOVERY")
    print("-" * 40)
    discovery = DiscoveryAgent()
    steps = discovery.discover(job_id, gitlab_url, branch)
    
    if not steps:
        return "# RCA Report\n\nNo steps discovered from the job. Please check the Job ID and GitLab URL."
    
    # Phase 2: Lineage / Table Mapping
    print("\n[Phase 2] LINEAGE")
    print("-" * 40)
    
    # Load manifest if provided
    manifest_data = None
    if manifest_path and os.path.exists(manifest_path):
        with open(manifest_path, 'r') as f:
            manifest_data = json.load(f)
        print(f"  Loaded manifest with {len(manifest_data)} step mappings")
    
    # Get table mappings (Lineage API or manifest)
    task_keys = [s.task_key for s in steps]
    table_mapping = get_step_tables(job_id, task_keys, fallback_to_manifest=manifest_data)
    
    for key, tables in table_mapping.items():
        print(f"  {key}: Sources={tables.get('sources', [])} Targets={tables.get('targets', [])}")
    
    # Phase 3: Parsing
    print("\n[Phase 3] PARSING")
    print("-" * 40)
    parser = PipelineParser()
    parsed_steps = parser.parse_all(steps, table_mapping=table_mapping)
    
    for ps in parsed_steps:
        print(f"  {ps.task_key}: {ps.logic_type} | Sources: {ps.source_tables} | Targets: {ps.target_tables}")
    
    # Phase 3: Validation
    print("\n[Phase 3] VALIDATION")
    print("-" * 40)
    validator = ValidationEngine(spark_session)
    anomalies = validator.validate_all(job_id, parsed_steps)
    
    # Initialize report sections
    rca_reports = []
    performance_reports = []
    
    if anomalies:
        # Phase 4: RCA Investigation
        print(f"\n[Phase 4] RCA INVESTIGATION ({len(anomalies)} anomalies)")
        print("-" * 40)
        rca = RCAAgent()
        rca_reports = rca.analyze_all(anomalies)
    else:
        print("\n[Result] No anomalies detected.")
    
    # Phase 5: Performance Analysis (Optional)
    if analyze_performance:
        print(f"\n[Phase 5] PERFORMANCE ANALYSIS")
        print("-" * 40)
        perf_agent = PerformanceAgent()
        
        for ps in parsed_steps:
            print(f"[Perf Agent] Analyzing: {ps.task_key}...")
            try:
                context = PerformanceContext(
                    step_name=ps.task_key,
                    source_tables=ps.source_tables,
                    target_tables=ps.target_tables,
                    code_content=ps.code_content,
                    execution_time_seconds=0  # Would come from job metadata
                )
                perf_report = perf_agent.analyze(context)
                performance_reports.append(f"## Performance: {ps.task_key}\n\n{perf_report}")
            except Exception as e:
                print(f"  -> Error: {e}")
    
    # Phase 6: Report Generation
    print("\n[Phase 6] REPORT GENERATION")
    print("-" * 40)
    
    final_report = f"""# Agentic RCA Report
**Generated**: {datetime.now().isoformat()}
**Job ID**: {job_id}
**Repository**: {gitlab_url}
**Branch**: {branch}

---

## Executive Summary
- **Steps Analyzed**: {len(parsed_steps)}
- **Anomalies Detected**: {len(anomalies)}
- **Performance Analysis**: {'Included' if analyze_performance else 'Skipped'}

---

"""
    
    # Add RCA findings
    if rca_reports:
        final_report += "# Root Cause Analysis\n\n"
        for report in rca_reports:
            final_report += report + "\n\n---\n\n"
    else:
        final_report += "## Status: All Clear\n\nNo anomalies detected. All steps are within expected parameters.\n\n"
        final_report += "### Steps Validated\n"
        for ps in parsed_steps:
            final_report += f"- **{ps.task_key}**: {ps.logic_type}\n"
        final_report += "\n---\n\n"
    
    # Add Performance findings
    if performance_reports:
        final_report += "# Performance Analysis\n\n"
        for report in performance_reports:
            final_report += report + "\n\n---\n\n"
    
    # Save report if output path provided
    if output_path:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(final_report)
        print(f"Report saved to: {output_path}")
    
    return final_report


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Agentic RCA System for Databricks ETL Pipelines",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --job-id 12345 --gitlab-url https://gitlab.com/org/repo.git
  python main.py --job-id 12345 --gitlab-url https://gitlab.com/org/repo.git --analyze-performance
        """
    )
    
    parser.add_argument(
        "--job-id",
        type=int,
        required=True,
        help="Databricks Job ID to analyze"
    )
    
    parser.add_argument(
        "--gitlab-url",
        type=str,
        required=True,
        help="GitLab repository URL (e.g., https://gitlab.com/org/repo.git)"
    )
    
    parser.add_argument(
        "--branch",
        type=str,
        default="main",
        help="Git branch to analyze (default: main)"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        default="rca_report.md",
        help="Output path for the RCA report (default: rca_report.md)"
    )
    
    parser.add_argument(
        "--analyze-performance",
        action="store_true",
        help="Enable performance analysis (query plans, skew detection)"
    )
    
    parser.add_argument(
        "--manifest",
        type=str,
        default=None,
        help="Path to JSON manifest with explicit table mappings per step"
    )
    
    args = parser.parse_args()
    
    # Run the pipeline
    report = run_rca_pipeline(
        job_id=args.job_id,
        gitlab_url=args.gitlab_url,
        branch=args.branch,
        output_path=args.output,
        analyze_performance=args.analyze_performance,
        manifest_path=args.manifest
    )
    
    # Print summary
    print("\n" + "=" * 60)
    print("       RCA COMPLETE")
    print("=" * 60)
    print(f"Full report saved to: {args.output}")


if __name__ == "__main__":
    main()

