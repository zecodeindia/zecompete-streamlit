"""
location_focused_keywords.py - Generate location-specific keywords from business data

This module creates keywords by pairing business names with their locations,
extracted from either the business name itself or the address field.
"""

import re
import pandas as pd
from typing import List, Dict, Set, Optional
from pinecone import Pinecone
from openai import OpenAI
from src.config import secret

def extract_locations_from_business_data(index_name: str = "zecompete") -> List[Dict]:
    """
    Extract business names and locations from Pinecone maps namespace
    
    Returns:
        List of dictionaries with business information including name and location
    """
    # Initialize Pinecone
    pc = Pinecone(api_key=secret("PINECONE_API_KEY"))
    index = pc.Index(index_name)
    
    # Get stats to determine dimension
    stats = index.describe_index_stats()
    dimension = stats.get("dimension", 1536)
    
    # Create dummy vector for query
    dummy_vector = [0.0] * dimension
    
    # Query the maps namespace
    results = index.query(
        vector=dummy_vector,
        top_k=100,  # Get up to 100 businesses
        namespace="maps",
        include_metadata=True
    )
    
    businesses = []
    if results and results.matches:
        for match in results.matches:
            if match.metadata:
                business = {}
                
                # Get business name
                business["name"] = match.metadata.get("name", "")
                
                # Get location from address if available
                address = match.metadata.get("address", "")
                city = match.metadata.get("city", "")
                
                # Extract location from business name
                business["extracted_location"] = extract_location_from_name(business["name"])
                
                # If no location in name, try to extract from address
                if not business["extracted_location"] and address:
                    business["extracted_location"] = extract_location_from_address(address)
                
                # Store other metadata
                business["address"] = address
                business["city"] = city
                business["brand"] = match.metadata.get("brand", "")
                
                # Only add businesses with names
                if business["name"]:
                    businesses.append(business)
    
    print(f"Extracted {len(businesses)} businesses with location data")
    return businesses

def extract_location_from_name(name: str) -> Optional[str]:
    """
    Extract location from a business name
    Example: "ZECODE Indiranagar" -> "Indiranagar"
    """
    if not name:
        return None
    
    # Common location patterns in business names
    # Business Name - Location
    hyphen_match = re.search(r'\s+-\s+(.+?)(?:,|\s+(?:Branch|Store|Outlet|Shop|Center|Centre)|\s*$)', name)
    if hyphen_match:
        return hyphen_match.group(1).strip()
    
    # Business Name, Location
    comma_match = re.search(r',\s+(.+?)(?:,|\s+(?:Branch|Store|Outlet|Shop|Center|Centre)|\s*$)', name)
    if comma_match:
        return comma_match.group(1).strip()
    
    # Business Name Location
    # This is trickier, try to identify common location names or patterns
    
    # List of known location indicators in business names
    location_indicators = [
        r'(?:in|at)\s+([A-Za-z\s]+?(?:Nagar|Layout|Road|Colony|Extension|Park|Plaza|Mall|Market|Place))',
        r'([A-Za-z\s]+?(?:Nagar|Layout|Road|Colony|Extension|Park|Plaza|Mall|Market|Place))\s+(?:Branch|Store|Outlet|Shop)',
        r'\s([A-Za-z\s]+?(?:Nagar|Layout|Road|Colony|Extension|Park|Plaza|Mall|Market|Place))'
    ]
    
    for pattern in location_indicators:
        match = re.search(pattern, name)
        if match:
            return match.group(1).strip()
    
    return None

def extract_location_from_address(address: str) -> Optional[str]:
    """
    Extract location from an address
    Looks for key areas or landmarks
    """
    if not address:
        return None
    
    # Look for key location patterns in addresses
    location_patterns = [
        r'([A-Za-z\s]+?(?:Nagar|Layout|Road|Colony|Extension|Park|Plaza|Mall|Market|Place))',
        r'([A-Za-z\s]+?(?:Phase|Sector|Block)[\s-]+[0-9]+)',
        r'([A-Za-z\s]+?(?:Industrial|Commercial|Residential)\s+(?:Area|Complex|Zone))',
    ]
    
    for pattern in location_patterns:
        matches = re.findall(pattern, address)
        if matches:
            # Return the first match that's not just "Road" or "Nagar" etc.
            for match in matches:
                if len(match.split()) > 1:  # Ensure it's not just "Road" alone
                    return match.strip()
    
    # If no specific location found, try splitting by commas and return a meaningful segment
    parts = [p.strip() for p in address.split(',')]
    for part in parts:
        if len(part.split()) >= 2 and len(part.split()) <= 4:  # Reasonable location name length
            if not any(word.isdigit() for word in part.split()):  # Avoid parts with numbers
                return part
    
    return None

def clean_brand_name(name: str) -> str:
    """
    Extract the core brand name by removing location and other qualifiers
    Example: "ZECODE Indiranagar Branch" -> "ZECODE"
    """
    if not name:
        return ""
    
    # Remove common suffixes
    cleaned = re.sub(r'\s+(?:Branch|Store|Outlet|Shop|Center|Centre).*$', '', name)
    
    # Remove location indicators
    cleaned = re.sub(r'\s+-\s+.*$', '', cleaned)
    cleaned = re.sub(r',\s+.*$', '', cleaned)
    
    # Remove common location patterns
    cleaned = re.sub(r'\s+(?:in|at)\s+[A-Za-z\s]+?(?:Nagar|Layout|Road|Colony|Extension|Park|Plaza|Mall|Market|Place).*$', '', cleaned)
    cleaned = re.sub(r'\s+[A-Za-z\s]+?(?:Nagar|Layout|Road|Colony|Extension|Park|Plaza|Mall|Market|Place).*$', '', cleaned)
    
    # Get first 1-3 words as the brand name (heuristic)
    words = cleaned.split()
    if len(words) > 3:
        return ' '.join(words[:3])
    
    return cleaned.strip()

def create_location_focused_keywords(business_data: List[Dict]) -> List[str]:
    """
    Create keywords by pairing brand names with locations
    
    Args:
        business_data: List of business dictionaries with name and location info
        
    Returns:
        List of brand+location keywords
    """
    keywords = set()
    
    for business in business_data:
        name = business.get("name", "")
        if not name:
            continue
            
        # Get brand and location
        brand = clean_brand_name(name)
        location = business.get("extracted_location")
        city = business.get("city", "")
        
        # Skip if no brand name
        if not brand:
            continue
        
        # Create brand + location keywords
        if location:
            # Base combination
            keywords.add(f"{brand} {location}")
            
            # Add variants
            keywords.add(f"{brand} {location} location")
            keywords.add(f"{brand} {location} address")
            keywords.add(f"{brand} {location} contact")
            keywords.add(f"{brand} {location} timings")
            keywords.add(f"{brand} {location} phone number")
            
            # If there's a city, add city variants only if city is not in location
            if city and city.lower() not in location.lower():
                keywords.add(f"{brand} {location} {city}")
        else:
            # If no location but we have city
            if city:
                keywords.add(f"{brand} {city}")
                keywords.add(f"{brand} in {city}")
                keywords.add(f"{brand} {city} location")
                keywords.add(f"{brand} {city} address")
    
    # Filter out empty or too short keywords
    filtered_keywords = [kw for kw in keywords if kw and len(kw) > 5]
    
    print(f"Generated {len(filtered_keywords)} location-focused keywords")
    return sorted(filtered_keywords)

def generate_location_focused_keywords() -> List[str]:
    """
    Main function to generate location-focused keywords from business data
    
    Returns:
        List of keywords focused on brand+location pairs
    """
    # Extract business data from Pinecone
    business_data = extract_locations_from_business_data()
    
    if not business_data:
        print("No business data found in Pinecone")
        return []
    
    # Create and return keywords
    return create_location_focused_keywords(business_data)

def enhance_with_openai(keywords: List[str], city: str) -> List[str]:
    """
    Enhance the core brand+location keywords with additional relevant variations
    using OpenAI (optional - use only if you want more variety)
    
    Args:
        keywords: Base keywords list (brand+location pairs)
        city: Default city name
        
    Returns:
        Enhanced list of keywords
    """
    if not keywords:
        return []
    
    # Get OpenAI client
    client = OpenAI(api_key=secret("OPENAI_API_KEY"))
    
    # Sample keywords to send to OpenAI (limit to 10 for efficiency)
    sample_keywords = keywords[:10]
    
    # Create prompt for OpenAI
    prompt = f"""
    I have the following brand+location keywords:
    
    {', '.join(sample_keywords)}
    
    Please generate 10-15 more similar keywords following exactly the same pattern:
    - Each keyword should pair a brand name with a specific location
    - Use the same format as the examples
    - Focus only on locations in {city}
    - Include search intents like "address", "timings", "location", "phone number" etc.
    
    Return only the list of keywords, one per line.
    """
    
    try:
        # Call OpenAI
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",  # Use a cheaper model for this task
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=200
        )
        
        # Process response
        if response.choices and response.choices[0].message.content:
            new_keywords = [
                line.strip() for line in response.choices[0].message.content.split("\n")
                if line.strip() and len(line.strip()) > 5
            ]
            
            # Combine with original keywords and deduplicate
            all_keywords = list(set(keywords + new_keywords))
            
            print(f"Enhanced keywords list with {len(all_keywords) - len(keywords)} additional keywords")
            return sorted(all_keywords)
        
    except Exception as e:
        print(f"Error enhancing keywords with OpenAI: {e}")
    
    # Return original keywords if enhancement fails
    return keywords

if __name__ == "__main__":
    # Test the keyword generation
    keywords = generate_location_focused_keywords()
    
    print("\nGenerated Keywords:")
    for kw in keywords[:20]:  # Print first 20 for sample
        print(f"- {kw}")
    
    # Optionally enhance with OpenAI
    # enhanced = enhance_with_openai(keywords, "Bengaluru")
