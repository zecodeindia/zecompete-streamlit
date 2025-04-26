"""
improved_keyword_generation.py - Enhanced keyword research using OpenAI

This module provides improved keyword generation functions that leverage OpenAI's
capabilities to generate more relevant, diverse, and high-quality keywords for local businesses.
"""

from typing import List, Dict, Set, Tuple
import re
import unicodedata
import json
import time
from openai import OpenAI
from src.config import secret

# Initialize OpenAI client
client = OpenAI(api_key=secret("OPENAI_API_KEY"))

# Constants
DEFAULT_MODEL = "gpt-4o-mini"  # Can be upgraded to gpt-4o for better results

def normalize_text(text: str) -> str:
    """Normalize text for comparison"""
    return unicodedata.normalize("NFKD", re.sub(r"\W", "", text.lower()))

def generate_local_search_keywords(
    business_names: List[str], 
    city: str,
    model: str = DEFAULT_MODEL
) -> List[str]:
    # ... existing code ...
    
    # Filter to only include brand+location pairs, without additional terms
    final_keywords = []
    forbidden_terms = ["location", "direction", "address", "timings", "timing", 
                      "phone", "number", "contact", "open", "hour", "time"]
    
    for kw in all_keywords:
        # Only include if:
        # 1. Contains a business name token
        # 2. Doesn't contain any forbidden terms
        # 3. Is not too short or too long
        if (any(token in normalize_text(kw) for token in business_tokens) and 
                not any(term in kw.lower() for term in forbidden_terms) and 
                5 < len(kw) < 50):
            # Add clean versions with city
            if city.lower() not in kw.lower() and "near me" not in kw.lower():
                kw_with_city = f"{kw} {city}"
                final_keywords.append(kw_with_city)
            final_keywords.append(kw)
    
    print(f"Generated {len(final_keywords)} clean brand+location keywords")
    return sorted(set(final_keywords))
    
    # Create a system prompt that guides the model to generate diverse, realistic keywords
    system_prompt = f"""You are an SEO and local search expert. Your task is to generate realistic search queries 
that people would type into Google when looking for specific businesses in {city}.

For each business, generate 5 different types of search queries:
1. Basic business name search (e.g., "business name")
2. Business name + location queries (e.g., "business in {city}", "business {city} location")
3. Business name + need-based queries (e.g., "business nearest store", "business open hours")
4. Business name + product/service queries (e.g., "business products", "business offerings")
5. Business name + comparison queries (e.g., "business vs competitor", "business alternatives")

For each business, provide exactly 5 queries total - one of each type, formatted as a list.
Ensure all queries are realistic, contain the business name, and would have search volume.
Focus on what real users would actually search for.
"""

    # Batch processing for efficiency
    batch_size = 5
    all_keywords = set()
    business_tokens = {normalize_text(name) for name in business_names}
    
    for i in range(0, len(business_names), batch_size):
        batch = business_names[i:i+batch_size]
        
        # Create user prompt for this batch
        user_prompt = f"Generate realistic search queries for these businesses in {city}:\n\n"
        for j, name in enumerate(batch, 1):
            user_prompt += f"{j}. {name}\n"
            
        # Add clear instructions for formatting
        user_prompt += f"\nFor each business, provide exactly 5 realistic search queries that people would type into Google. Make sure each query contains the business name or a recognizable part of it."
        
        try:
            # Call OpenAI API
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.7,  # Higher creativity for diverse queries
                max_tokens=1500
            )
            
            # Extract keywords from response
            keyword_text = response.choices[0].message.content
            
            # Parse different response formats
            if "1." in keyword_text and ":" in keyword_text:
                # Response likely in a business: keywords format
                for line in keyword_text.split("\n"):
                    if ":" in line and any(name.lower() in line.lower() for name in batch):
                        # Extract keywords after colon
                        _, keywords_part = line.split(":", 1)
                        
                        # Split by various separators
                        for separator in [",", "•", "-", ";"]:
                            if separator in keywords_part:
                                for kw in keywords_part.split(separator):
                                    kw = kw.strip().strip('"\'').strip()
                                    if kw and any(token in normalize_text(kw) for token in business_tokens):
                                        all_keywords.add(kw)
            else:
                # Simpler parsing - just extract lines with business names
                for line in keyword_text.split("\n"):
                    line = line.strip().strip('"\'•-').strip()
                    if line and any(name.lower() in line.lower() for name in batch):
                        all_keywords.add(line)
                        
            # Also check for bulleted or numbered lists
            pattern = r'[0-9•\-\*]+\.\s*"([^"]+)"'
            matches = re.findall(pattern, keyword_text)
            for match in matches:
                if match and any(token in normalize_text(match) for token in business_tokens):
                    all_keywords.add(match)
            
            # Add a small delay to avoid rate limits
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Error generating keywords for batch: {e}")
            continue
    
    # Final filtering to ensure only relevant keywords
    final_keywords = []
    for kw in all_keywords:
        # Only include if it contains a business name token and isn't too generic
        if any(token in normalize_text(kw) for token in business_tokens) and len(kw) > 5:
            # Add city name if not already present and not a "near me" query
            if city.lower() not in kw.lower() and "near me" not in kw.lower():
                kw_with_city = f"{kw} {city}"
                final_keywords.append(kw_with_city)
            final_keywords.append(kw)
    
    print(f"Generated {len(final_keywords)} unique keywords")
    return sorted(set(final_keywords))

def generate_competitor_keywords(
    business_names: List[str],
    city: str,
    competitors: List[str] = None,
    model: str = DEFAULT_MODEL
) -> List[str]:
    """
    Generate competitor comparison keywords
    
    Args:
        business_names: List of main business names
        city: Target city
        competitors: List of competitor names (optional)
        model: OpenAI model to use
        
    Returns:
        List of competitor comparison keywords
    """
    if not business_names:
        return []
        
    if not competitors:
        # Auto-generate competitor set
        try:
            competitor_prompt = f"""Based on these businesses in {city}:
{', '.join(business_names[:5])}

List 5 major competitors in this industry that would operate in {city}.
Provide only the names, separated by commas."""

            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": competitor_prompt}],
                temperature=0.7,
                max_tokens=150
            )
            
            competitor_text = response.choices[0].message.content
            competitors = [c.strip() for c in re.split(r'[,\n]', competitor_text) if c.strip()]
            print(f"Auto-generated competitors: {competitors}")
        except Exception as e:
            print(f"Error generating competitors: {e}")
            competitors = []
    
    # Generate comparison keywords
    comparison_keywords = []
    
    for business in business_names[:5]:  # Limit to first 5 businesses for efficiency
        for competitor in competitors:
            comparison_keywords.append(f"{business} vs {competitor}")
            comparison_keywords.append(f"{business} or {competitor}")
            comparison_keywords.append(f"{business} compared to {competitor}")
            comparison_keywords.append(f"{business} {competitor} difference")
            
    print(f"Generated {len(comparison_keywords)} competitor comparison keywords")
    return comparison_keywords

def generate_enhanced_keywords(
    business_names: List[str],
    city: str,
    include_competitor_keywords: bool = True,
    model: str = DEFAULT_MODEL
) -> List[str]:
    """
    Generate an enhanced set of keywords using multiple strategies
    
    Args:
        business_names: List of business names
        city: Target city
        include_competitor_keywords: Whether to include competitor comparison keywords
        model: OpenAI model to use
        
    Returns:
        Combined list of keywords from all strategies
    """
    # Get basic local search keywords
    basic_keywords = generate_local_search_keywords(business_names, city, model)
    
    # Get competitor keywords if requested
    competitor_keywords = []
    if include_competitor_keywords:
        competitor_keywords = generate_competitor_keywords(business_names, city, None, model)
    
    # Combine and deduplicate
    all_keywords = list(set(basic_keywords + competitor_keywords))
    
    print(f"Generated {len(all_keywords)} total keywords")
    return all_keywords

if __name__ == "__main__":
    # Example usage
    test_businesses = ["ZECODE", "H&M", "Zara"]
    test_city = "Bengaluru"
    
    keywords = generate_enhanced_keywords(test_businesses, test_city)
    
    print("\nSample Keywords:")
    for kw in keywords[:20]:
        print(f"- {kw}")
