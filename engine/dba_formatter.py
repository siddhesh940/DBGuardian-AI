"""
DBA Output Formatter
Formats DBA Expert Analysis for clean, professional output
"""


class DBAFormatter:
    """
    Formats DBA Expert Engine output into clean, readable format
    """
    
    @staticmethod
    def format_for_api(dba_analysis: dict) -> dict:
        """
        Format DBA analysis for API response
        Returns clean, structured output for frontend
        """
        
        workload = dba_analysis.get('workload_summary', {})
        findings = dba_analysis.get('problematic_sql_findings', [])
        conclusion = dba_analysis.get('dba_conclusion', '')
        
        # Format workload summary
        workload_formatted = {
            "pattern": workload.get('pattern', 'UNKNOWN'),
            "total_elapsed_s": workload.get('total_elapsed', 0),
            "total_cpu_s": workload.get('total_cpu', 0),
            "total_executions": workload.get('total_executions', 0),
            "sql_analyzed": workload.get('sql_count', 0),
            "problematic_found": len(findings),
            "dominant_wait_event": workload.get('dominant_wait', {}).get('event') if workload.get('dominant_wait') else None
        }
        
        # Format findings (problematic SQL only) with all enhanced fields
        findings_formatted = []
        for finding in findings:
            findings_formatted.append({
                # Basic identification
                "sql_id": finding.get('sql_id'),
                "severity": finding.get('severity'),
                "priority_score": finding.get('dba_priority_score'),
                
                # 1️⃣ Summary
                "problem_summary": finding.get('problem_summary', ''),
                
                # 2️⃣ Technical Parameters
                "technical_parameters": finding.get('technical_parameters', {}),
                
                # 3️⃣ Execution Pattern
                "execution_pattern": finding.get('execution_pattern', {}),
                
                # 4️⃣ DBA Interpretation
                "dba_interpretation": finding.get('dba_interpretation', ''),
                
                # User-friendly explanation
                "explanation": finding.get('explanation', ''),
                
                # 5️⃣ Wait/ASH Linkage
                "wait_linkage": finding.get('wait_linkage', ''),
                
                # 🛠️ Final Recommendations
                "recommendations": finding.get('dba_recommendations', {}),
                
                # 🔥 LOAD REDUCTION ACTIONS - PRIMARY OUTPUT
                "load_reduction_actions": finding.get('load_reduction_actions', {}),
                
                # SQL preview
                "sql_text_preview": finding.get('sql_text_preview', '')
            })
        
        return {
            "dba_analysis_type": "AI_DRIVEN_SENIOR_DBA",
            "analysis_approach": "DEEP_ANALYSIS_PROBLEMATIC_ONLY",
            "analysis_completeness": {
                "problematic_sql_count": dba_analysis.get('problematic_count', 0),
                "total_sql_analyzed": dba_analysis.get('total_analyzed', 0),
                "filter_applied": "ONLY_1_OR_2_MOST_PROBLEMATIC"
            },
            "workload_summary": workload_formatted,
            "problematic_sql_findings": findings_formatted,
            "dba_final_conclusion": conclusion
        }
    
    @staticmethod
    def format_for_console(dba_analysis: dict) -> str:
        """
        Format DBA analysis for console output
        Returns formatted string for terminal display
        """
        
        workload = dba_analysis.get('workload_summary', {})
        findings = dba_analysis.get('problematic_sql_findings', [])
        conclusion = dba_analysis.get('dba_conclusion', '')
        
        output = []
        
        # Header
        output.append("=" * 100)
        output.append("� AI DBA INTELLIGENCE & EXPERT RECOMMENDATIONS")
        output.append("Deep Analysis • Confident • Analytical")
        output.append("=" * 100)
        output.append("")
        
        # Workload Summary
        output.append("📊 WORKLOAD SUMMARY")
        output.append("-" * 100)
        output.append(f"Pattern: {workload.get('pattern', 'UNKNOWN').replace('_', ' ')}")
        output.append(f"Total Elapsed: {workload.get('total_elapsed', 0):.1f}s")
        output.append(f"Total CPU: {workload.get('total_cpu', 0):.1f}s")
        output.append(f"Total Executions: {workload.get('total_executions', 0):,}")
        output.append(f"SQL Analyzed: {workload.get('sql_count', 0)}")
        output.append(f"Problematic SQL Found: {len(findings)}")
        
        if workload.get('dominant_wait'):
            output.append(f"Dominant Wait: {workload['dominant_wait'].get('event')} ({workload['dominant_wait'].get('pct_db_time', 0):.1f}% DB time)")
        
        output.append("")
        
        # Findings - ONLY 1-2 Most Problematic
        if findings:
            output.append("🔥 PROBLEMATIC SQL IDENTIFIED (ONLY THE MOST CRITICAL)")
            output.append(f"Analysis scope: {workload.get('sql_count', 0)} queries → {len(findings)} problematic")
            output.append("=" * 100)
            output.append("")
            
            for idx, finding in enumerate(findings, 1):
                output.append(f"{'=' * 100}")
                output.append(f"🎯 FINDING #{idx} - SQL_ID: {finding.get('sql_id')}")
                output.append(f"{'=' * 100}")
                output.append("")
                
                # 1️⃣ Problem Summary
                output.append(finding.get('problem_summary', ''))
                output.append("")
                
                # 2️⃣ Technical Performance Parameters
                output.append("2️⃣ TECHNICAL PERFORMANCE PARAMETERS")
                output.append("-" * 100)
                params = finding.get('technical_parameters', {})
                output.append(f"  SQL ID:              {params.get('sql_id', 'Unknown')}")
                output.append(f"  Total Elapsed Time:  {params.get('total_elapsed_time_s', 0):.2f}s")
                output.append(f"  CPU Time:            {params.get('cpu_time_s', 0):.2f}s")
                output.append(f"  Execution Count:     {params.get('executions', 0):,}")
                output.append(f"  Avg Elapsed/Exec:    {params.get('avg_elapsed_per_exec_s', 0):.4f}s")
                output.append(f"  Contribution % DB:   {params.get('contribution_to_db_time_pct', 0):.2f}%")
                output.append(f"  CPU %:               {params.get('cpu_percentage', 0):.2f}%")
                output.append(f"  I/O %:               {params.get('io_percentage', 0):.2f}%")
                output.append("")
                
                # 3️⃣ Execution Pattern Understanding
                output.append("3️⃣ EXECUTION PATTERN UNDERSTANDING")
                output.append("-" * 100)
                pattern = finding.get('execution_pattern', {})
                output.append(f"Pattern Type: {pattern.get('pattern_type', 'UNKNOWN')}")
                output.append(f"{pattern.get('description', '')}")
                output.append("")
                output.append(f"Is High Frequency?  {'Yes' if pattern.get('is_high_frequency') else 'No'}")
                output.append(f"Is Bursty?          {'Yes' if pattern.get('is_bursty') else 'No'}")
                output.append(f"Is Sustained Load?  {'Yes' if pattern.get('is_sustained') else 'No'}")
                output.append("")
                output.append("📝 DBA Assessment:")
                output.append(pattern.get('dba_assessment', ''))
                output.append("")
                
                # 4️⃣ Possible DBA Interpretation
                output.append("4️⃣ POSSIBLE DBA INTERPRETATION")
                output.append("-" * 100)
                output.append(finding.get('dba_interpretation', ''))
                output.append("")
                
                # 5️⃣ Wait / ASH Linkage
                output.append("5️⃣ WAIT / ASH LINKAGE")
                output.append("-" * 100)
                output.append(finding.get('wait_linkage', ''))
                output.append("")
                
                # 🛠️ FINAL DBA RECOMMENDATION
                output.append("🛠️ FINAL DBA RECOMMENDATION SECTION")
                output.append("=" * 100)
                recs = finding.get('dba_recommendations', {})
                
                # 1️⃣ Tuning Priority
                output.append("")
                output.append("1️⃣ TUNING PRIORITY")
                output.append(recs.get('priority_description', 'UNKNOWN'))
                output.append("")
                
                # 2️⃣ What DBA should do next
                output.append(recs.get('what_dba_should_do_next', ''))
                
                # 3️⃣ Short DBA Action Plan
                output.append(recs.get('dba_action_plan', ''))
                
                # Expected improvement
                output.append(f"💡 {recs.get('expected_improvement', '')}")
                output.append("")
                
                # SQL Text Preview
                sql_preview = finding.get('sql_text_preview', '')
                if sql_preview and sql_preview != 'SQL text not available':
                    output.append("📝 SQL TEXT PREVIEW")
                    output.append("-" * 100)
                    output.append(sql_preview[:500])  # Limit to 500 chars
                    if len(sql_preview) > 500:
                        output.append("... (truncated)")
                    output.append("")
                
                output.append("")
        else:
            output.append("✅ NO CRITICAL ISSUES FOUND")
            output.append("All SQL queries performing within acceptable parameters")
            output.append("NO EXTRA QUERIES • NO LOW RISK ITEMS")
            output.append("")
        
        # Final Conclusion - DBA to DBA tone
        output.append("=" * 100)
        output.append("🎯 FINAL DBA CONCLUSION")
        output.append("=" * 100)
        output.append(conclusion)
        output.append("")
        
        return "\n".join(output)
    
    @staticmethod
    def format_summary_only(dba_analysis: dict) -> dict:
        """
        Format only summary information (for dashboard view)
        """
        
        workload = dba_analysis.get('workload_summary', {})
        findings = dba_analysis.get('problematic_sql_findings', [])
        
        summary_items = []
        for finding in findings:
            summary_items.append({
                "sql_id": finding.get('sql_id'),
                "severity": finding.get('severity'),
                "priority_score": finding.get('dba_priority_score'),
                "elapsed_s": finding.get('technical_parameters', {}).get('total_elapsed_time_s', 0),
                "cpu_s": finding.get('technical_parameters', {}).get('cpu_time_s', 0),
                "executions": finding.get('technical_parameters', {}).get('execution_count', 0)
            })
        
        return {
            "problematic_count": len(findings),
            "total_analyzed": dba_analysis.get('total_analyzed', 0),
            "workload_pattern": workload.get('pattern'),
            "summary_items": summary_items
        }
