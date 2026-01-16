"""
Deterministic structure enforcer for executive summaries.
Forces the 9-section structure regardless of what the LLM generates.
"""
import re
from typing import Dict, List


EXPECTED_SECTIONS = [
    "Performance Overview",
    "Channel & Platform Analysis",
    "Funnel & Strategic Insights",
    "Ad Type & Creative Performance",
    "Audience & Demographic Insights",
    "What Is Working",
    "What Is Not Working",
    "Budget Optimization",
    "Priority Actions"
]


def enforce_structure(detailed_summary: str) -> str:
    """
    Force the detailed summary to have all 9 required sections.
    
    If the LLM didn't generate a section, we'll extract relevant content
    or add a placeholder.
    
    Args:
        detailed_summary: Raw LLM output
        
    Returns:
        Structured summary with all 9 sections
    """
    # Try to extract existing sections
    sections = _extract_sections(detailed_summary)
    
    # Build the structured output
    structured_output = []
    
    for section_name in EXPECTED_SECTIONS:
        # Use markdown bolding for the header - format compatible with frontend rendering
        structured_output.append(f"\n**{section_name}:**\n")
        
        if section_name in sections and sections[section_name].strip():
            structured_output.append(sections[section_name].strip())
        else:
            # Section missing - add placeholder
            structured_output.append(f"Analysis for {section_name} not available in current data.")
        
        structured_output.append("\n")
    
    return "\n".join(structured_output)


def _extract_sections(text: str) -> Dict[str, str]:
    """
    Extract sections from LLM output by looking for section headers.
    
    Returns dict mapping section name to content.
    """
    sections = {}
    
    # Try to find each expected section
    for i, section_name in enumerate(EXPECTED_SECTIONS):
        # Look for this section header (case-insensitive, with optional formatting like **Header:**)
        pattern = rf'(?:^|\n)\s*(?:\*\*)?{re.escape(section_name)}(?:\*\*)?\s*[:\-]?\s*\n'
        matches = list(re.finditer(pattern, text, re.IGNORECASE | re.MULTILINE))
        
        if matches:
            # Found the section header
            start = matches[0].end()
            
            # Find where this section ends (next section header or end of text)
            end = len(text)
            for next_section in EXPECTED_SECTIONS[i+1:]:
                next_pattern = rf'(?:^|\n)\s*(?:\*\*)?{re.escape(next_section)}(?:\*\*)?\s*[:\-]?\s*\n'
                next_matches = list(re.finditer(next_pattern, text[start:], re.IGNORECASE | re.MULTILINE))
                if next_matches:
                    end = start + next_matches[0].start()
                    break
            
            content = text[start:end].strip()
            sections[section_name] = content
        else:
            # Section not found - try to extract relevant content by keywords
            content = _extract_by_keywords(text, section_name)
            if content:
                sections[section_name] = content
    
    return sections


def _extract_by_keywords(text: str, section_name: str) -> str:
    """
    Try to extract relevant content for a section based on keywords.
    """
    # Define keywords for each section
    keyword_map = {
        "Performance Overview": ["spend", "total", "campaign", "platform", "overall"],
        "Channel & Platform Analysis": ["channel", "platform", "Google", "Meta", "LinkedIn", "TikTok", "top", "bottom", "ranking"],
        "Funnel & Strategic Insights": ["funnel", "awareness", "conversion", "interest", "stage", "drop-off"],
        "Ad Type & Creative Performance": ["creative", "ad type", "video", "image", "carousel", "ctr", "cost-effective"],
        "Audience & Demographic Insights": ["audience", "segment", "demographic", "responding", "target"],
        "What Is Working": ["working", "success", "top performer", "best", "strong"],
        "What Is Not Working": ["not working", "underperform", "poor", "weak", "issue"],
        "Budget Optimization": ["budget", "allocation", "shift", "reallocate", "spend"],
        "Priority Actions": ["action", "recommendation", "priority", "immediate", "should"]
    }
    
    keywords = keyword_map.get(section_name, [])
    
    # Find sentences containing these keywords
    sentences = re.split(r'[.!?]\s+', text)
    relevant_sentences = []
    
    for sentence in sentences:
        sentence_lower = sentence.lower()
        if any(keyword.lower() in sentence_lower for keyword in keywords):
            relevant_sentences.append(sentence.strip())
            if len(relevant_sentences) >= 3:  # Max 3 sentences per section
                break
    
    if relevant_sentences:
        return ". ".join(relevant_sentences) + "."
    
    return ""
