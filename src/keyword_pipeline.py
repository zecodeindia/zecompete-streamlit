"""
keyword_pipeline.py - Generate keywords from business names and get search volume data
"""
import pandas as pd
from typing import List, Dict, Optional
from pinecone import Pinecone
from openai import OpenAI
from src.config import secret
from src.fetch_volume import fetch_volume
from src.embed_upsert import upsert_keywords

def get_business_names_from_pinecone() -> List[str]:
    """Retrieve all business names from Pinecone index"""
    # Initialize Pinecone
    pc = Pinecone(api_key=secret("PINECONE_API_KEY"))
    index = pc.Index("zecompete")
    
    # Create a dummy query vector (filled with zeros) to fetch metadata
    dimension = 1536  # Standard OpenAI embedding dimension
    dummy_vector = [0] * dimension
    
    # Query with a large limit to get all records
    results = index.query(
        vector=dummy_vector,
        top_k=100,  # Adjust based on your data size
        include_metadata=True,
        namespace="maps"  # Use the namespace where your business data is stored
    )
    
    # Extract business names from metadata
    business_names = []
    for match in results.matches:
        if match.metadata and "name" in match.metadata:
            business_names.append(match.metadata["name"])
    
    # Remove duplicates
    business_names = list(set(business_names))
    print(f"Retrieved {len(business_names)} unique business names")
    
    return business_names

def generate_keywords_for_businesses(business_names: List[str]) -> List[str]:
    """Generate relevant keywords for a list of business names using OpenAI"""
    client = OpenAI(api_key=secret("OPENAI_API_KEY"))
    
    # Group businesses to minimize API calls (process in batches of 10)
    all_keywords = []
    
    for i in range(0, len(business_names), 10):
        batch = business_names[i:i+10]
        businesses_text = "\n".join([f"- {name}" for name in batch])
        
        prompt = f"""
        For the following businesses:
        {businesses_text}
        
        Generate a list of relevant search keywords that potential customers might use to find these businesses online.
        Focus on:
        - Generic product/service terms
        - Location-based searches
        - Brand-specific terms
        - Need-based queries
        
        Return ONLY the keywords as a comma-separated list.
        """
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7
        )
        
        # Parse keywords from response
        keywords_text = response.choices[0].message.content.strip()
        batch_keywords = [k.strip() for k in keywords_text.split(",")]
        
        # Add to master list
        all_keywords.extend(batch_keywords)
    
    # Remove duplicates and empty strings
    unique_keywords = list(set(k for k in all_keywords if k))
    print(f"Generated {len(unique_keywords)} unique keywords")
    
    return unique_keywords

def get_search_volumes(keywords: List[str]) -> pd.DataFrame:
    """Get search volume data for keywords"""
    # Process in batches of 100 keywords (DataForSEO limit)
    all_volume_data = []
    
    for i in range(0, len(keywords), 100):
        batch = keywords[i:i+100]
        print(f"Fetching search volume for batch {i//100 + 1} ({len(batch)} keywords)")
        
        try:
            volume_result = fetch_volume(batch)
            
            # Transform the result into a DataFrame
            rows = []
            for kw in volume_result:
                for m in kw["keyword_info"]["monthly_searches"]:
                    rows.append({
                        "keyword": kw["keyword"],
                        "year": m["year"],
                        "month": m["month"],
                        "search_volume": m["search_volume"]
                    })
            
            if rows:
                batch_df = pd.DataFrame(rows)
                all_volume_data.append(batch_df)
        except Exception as e:
            print(f"Error fetching volume data for batch: {str(e)}")
    
    # Combine all batches
    if all_volume_data:
        result_df = pd.concat(all_volume_data, ignore_index=True)
        print(f"Retrieved search volume data for {result_df['keyword'].nunique()} keywords")
        return result_df
    else:
        print("No search volume data retrieved")
        return pd.DataFrame(columns=["keyword", "year", "month", "search_volume"])

def run_keyword_pipeline(city: str = "General") -> bool:
    """Execute the full keyword pipeline"""
    try:
        # Step 1: Get business names from Pinecone
        business_names = get_business_names_from_pinecone()
        if not business_names:
            print("No business names found in Pinecone")
            return False
            
        # Step 2: Generate keywords using OpenAI
        keywords = generate_keywords_for_businesses(business_names)
        if not keywords:
            print("No keywords generated")
            return False
            
        # Step 3: Get search volume data from DataForSEO
        keywords_df = get_search_volumes(keywords)
        if keywords_df.empty:
            print("No search volume data retrieved")
            return False
            
        # Step 4: Store the keyword data in Pinecone
        upsert_keywords(keywords_df, city)
        
        print("Keyword pipeline completed successfully")
        return True
    except Exception as e:
        print(f"Error in keyword pipeline: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
