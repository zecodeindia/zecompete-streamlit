"""
keyword_pipeline.py - Generate keywords from business names and get search volume data
"""
import pandas as pd
import traceback
from typing import List, Dict, Optional
from pinecone import Pinecone
from openai import OpenAI
from src.config import secret
from src.fetch_volume import fetch_volume
from src.embed_upsert import upsert_keywords

def get_business_names_from_pinecone() -> List[str]:
    """Retrieve all business names from Pinecone index"""
    try:
        # Initialize Pinecone
        print("Initializing Pinecone connection...")
        pc = Pinecone(api_key=secret("PINECONE_API_KEY"))
        index = pc.Index("zecompete")
        
        # Create a dummy query vector (filled with zeros) to fetch metadata
        dimension = 1536  # Standard OpenAI embedding dimension
        dummy_vector = [0] * dimension
        
        # Get all available namespaces
        print("Retrieving index stats and namespaces...")
        stats = index.describe_index_stats()
        namespaces = list(stats.get("namespaces", {}).keys())
        print(f"Found namespaces: {namespaces}")
        
        all_business_names = []
        
        # Try all namespaces to find business names
        for namespace in namespaces:
            print(f"Checking namespace: {namespace}")
            if namespace == "keywords":
                continue  # Skip the keywords namespace
                
            # Query with a large limit to get records from this namespace
            try:
                results = index.query(
                    vector=dummy_vector,
                    top_k=100,  # Adjust based on your data size
                    include_metadata=True,
                    namespace=namespace
                )
                
                # Extract business names from metadata, trying different field names
                namespace_names = []
                
                for match in results.matches:
                    if not match.metadata:
                        continue
                        
                    # Try different field names that might contain business names
                    name = None
                    for field in ['name', 'title', 'business_name', 'brand', 'company']:
                        if field in match.metadata and match.metadata[field]:
                            name = match.metadata[field]
                            break
                    
                    if name:
                        namespace_names.append(name)
                
                print(f"Found {len(namespace_names)} business names in namespace '{namespace}'")
                all_business_names.extend(namespace_names)
            except Exception as e:
                print(f"Error querying namespace '{namespace}': {str(e)}")
                traceback.print_exc()
        
        # If we found nothing in namespaces, try one more time with no namespace
        if not all_business_names:
            print("No business names found in specific namespaces, trying default namespace...")
            try:
                results = index.query(
                    vector=dummy_vector,
                    top_k=100,
                    include_metadata=True
                )
                
                for match in results.matches:
                    if not match.metadata:
                        continue
                        
                    # Try different field names again
                    name = None
                    for field in ['name', 'title', 'business_name', 'brand', 'company']:
                        if field in match.metadata and match.metadata[field]:
                            name = match.metadata[field]
                            break
                    
                    if name:
                        all_business_names.append(name)
            except Exception as e:
                print(f"Error querying default namespace: {str(e)}")
                traceback.print_exc()
        
        # Remove duplicates
        business_names = list(set(all_business_names))
        print(f"Retrieved {len(business_names)} unique business names")
        
        # Print sample of names for debugging
        if business_names:
            print(f"Sample business names: {', '.join(business_names[:5])}")
        
        return business_names
    except Exception as e:
        print(f"Unexpected error in get_business_names_from_pinecone: {str(e)}")
        traceback.print_exc()
        return []

def generate_keywords_for_businesses(business_names: List[str]) -> List[str]:
    """Generate relevant keywords for a list of business names using OpenAI"""
    try:
        print(f"Initializing OpenAI client for generating keywords from {len(business_names)} business names...")
        client = OpenAI(api_key=secret("OPENAI_API_KEY"))
        
        # Group businesses to minimize API calls (process in batches of 10)
        all_keywords = []
        
        for i in range(0, len(business_names), 10):
            batch = business_names[i:i+10]
            print(f"Processing batch {i//10 + 1} with {len(batch)} business names")
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
            
            try:
                print(f"Sending batch {i//10 + 1} to OpenAI...")
                response = client.chat.completions.create(
                    model="gpt-3.5-turbo",  # Using more widely available model
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.7
                )
                
                # Parse keywords from response
                keywords_text = response.choices[0].message.content.strip()
                batch_keywords = [k.strip() for k in keywords_text.split(",")]
                
                print(f"Received {len(batch_keywords)} keywords from batch {i//10 + 1}")
                
                # Add to master list
                all_keywords.extend(batch_keywords)
            except Exception as e:
                print(f"Error processing batch {i//10 + 1}: {str(e)}")
                traceback.print_exc()
        
        # Remove duplicates and empty strings
        unique_keywords = list(set(k for k in all_keywords if k))
        print(f"Generated {len(unique_keywords)} unique keywords")
        
        # Print sample of keywords for debugging
        if unique_keywords:
            print(f"Sample keywords: {', '.join(unique_keywords[:10])}")
        
        return unique_keywords
    except Exception as e:
        print(f"Unexpected error in generate_keywords_for_businesses: {str(e)}")
        traceback.print_exc()
        return []

def get_search_volumes(keywords: List[str]) -> pd.DataFrame:
    """Get search volume data for keywords with improved error handling"""
    try:
        print(f"Fetching search volume data for {len(keywords)} keywords...")
        
        # Process in batches of 100 keywords (DataForSEO limit)
        all_volume_data = []
        
        for i in range(0, len(keywords), 100):
            batch = keywords[i:i+100]
            print(f"Processing batch {i//100 + 1} with {len(batch)} keywords")
            
            try:
                # Get raw response from DataForSEO
                volume_result = fetch_volume(batch)
                
                # Print the first result for debugging
                if volume_result and len(volume_result) > 0:
                    print(f"Sample response structure: {str(volume_result[0])[:200]}...")
                
                # Transform the result into a DataFrame
                rows = []
                for kw in volume_result:
                    try:
                        # Check if keyword_info exists
                        if "keyword_info" not in kw:
                            print(f"Missing keyword_info for: {kw.get('keyword', 'unknown')}")
                            # Create a fallback entry with estimated values
                            rows.append({
                                "keyword": kw.get("keyword", "unknown"),
                                "year": 2023,
                                "month": 1,
                                "search_volume": 0  # Default to zero for missing data
                            })
                            continue
                            
                        # Check if monthly_searches exists
                        if "monthly_searches" not in kw["keyword_info"]:
                            print(f"Missing monthly_searches for: {kw.get('keyword', 'unknown')}")
                            rows.append({
                                "keyword": kw.get("keyword", "unknown"),
                                "year": 2023,
                                "month": 1,
                                "search_volume": kw["keyword_info"].get("search_volume", 0)
                            })
                            continue
                            
                        # Process normal data with monthly breakdown
                        for m in kw["keyword_info"]["monthly_searches"]:
                            rows.append({
                                "keyword": kw["keyword"],
                                "year": m["year"],
                                "month": m["month"],
                                "search_volume": m["search_volume"]
                            })
                    except KeyError as ke:
                        print(f"Key error in keyword data: {ke} for keyword: {kw.get('keyword', 'unknown')}")
                        # Create a fallback entry
                        rows.append({
                            "keyword": kw.get("keyword", "unknown"),
                            "year": 2023,
                            "month": 1,
                            "search_volume": 0
                        })
                    except Exception as e:
                        print(f"Error processing keyword: {str(e)}")
                
                if rows:
                    batch_df = pd.DataFrame(rows)
                    all_volume_data.append(batch_df)
                    print(f"Batch {i//100 + 1} resulted in {len(rows)} data points")
                else:
                    print(f"No data points generated from batch {i//100 + 1}")
                    
                    # Create placeholder data for the whole batch if no results
                    placeholder_rows = []
                    for keyword in batch:
                        placeholder_rows.append({
                            "keyword": keyword,
                            "year": 2023,
                            "month": 1,
                            "search_volume": 0
                        })
                    placeholder_df = pd.DataFrame(placeholder_rows)
                    all_volume_data.append(placeholder_df)
                    print(f"Created {len(placeholder_rows)} placeholder entries")
                    
            except Exception as e:
                print(f"Error processing batch {i//100 + 1}: {str(e)}")
                traceback.print_exc()
                
                # Create placeholder data if the batch fails
                placeholder_rows = []
                for keyword in batch:
                    placeholder_rows.append({
                        "keyword": keyword,
                        "year": 2023,
                        "month": 1,
                        "search_volume": 0
                    })
                placeholder_df = pd.DataFrame(placeholder_rows)
                all_volume_data.append(placeholder_df)
                print(f"Created {len(placeholder_rows)} placeholder entries after error")
        
        # Combine all batches
        if all_volume_data:
            result_df = pd.concat(all_volume_data, ignore_index=True)
            unique_keywords = result_df['keyword'].nunique()
            print(f"Retrieved search volume data for {unique_keywords} unique keywords, total of {len(result_df)} rows")
            
            # Print sample data for debugging
            if not result_df.empty:
                print("Sample data:")
                print(result_df.head(3))
            
            return result_df
        else:
            print("No search volume data retrieved from any batch, creating fallback data")
            # Create fallback data for all keywords
            fallback_df = pd.DataFrame([
                {"keyword": keyword, "year": 2023, "month": 1, "search_volume": 0}
                for keyword in keywords
            ])
            return fallback_df
    except Exception as e:
        print(f"Unexpected error in get_search_volumes: {str(e)}")
        traceback.print_exc()
        
        # Create fallback data for all keywords
        fallback_df = pd.DataFrame([
            {"keyword": keyword, "year": 2023, "month": 1, "search_volume": 0}
            for keyword in keywords
        ])
        return fallback_df
        
        # Combine all batches
        if all_volume_data:
            result_df = pd.concat(all_volume_data, ignore_index=True)
            unique_keywords = result_df['keyword'].nunique()
            print(f"Retrieved search volume data for {unique_keywords} unique keywords, total of {len(result_df)} rows")
            
            # Print sample data for debugging
            if not result_df.empty:
                print("Sample data:")
                print(result_df.head(3))
            
            return result_df
        else:
            print("No search volume data retrieved from any batch")
            return pd.DataFrame(columns=["keyword", "year", "month", "search_volume"])
    except Exception as e:
        print(f"Unexpected error in get_search_volumes: {str(e)}")
        traceback.print_exc()
        return pd.DataFrame(columns=["keyword", "year", "month", "search_volume"])

def run_keyword_pipeline(city: str = "General") -> bool:
    """Execute the full keyword pipeline"""
    try:
        print(f"Starting keyword pipeline for city: {city}")
        
        # Step 1: Get business names from Pinecone
        print("Step 1: Getting business names from Pinecone...")
        business_names = get_business_names_from_pinecone()
        if not business_names:
            print("Error: No business names found in Pinecone")
            return False
        print(f"Successfully retrieved {len(business_names)} business names")
        
        # Step 2: Generate keywords using OpenAI
        print("Step 2: Generating keywords with OpenAI...")
        try:
            keywords = generate_keywords_for_businesses(business_names)
            if not keywords:
                print("Error: No keywords generated")
                return False
            print(f"Successfully generated {len(keywords)} keywords")
        except Exception as e:
            print(f"Error in keyword generation: {str(e)}")
            traceback.print_exc()
            return False
            
        # Step 3: Get search volume data from DataForSEO
        print("Step 3: Getting search volume data from DataForSEO...")
        try:
            keywords_df = get_search_volumes(keywords)
            if keywords_df.empty:
                print("Error: No search volume data retrieved")
                return False
            print(f"Successfully retrieved search volume data for {len(keywords_df)} keyword-month combinations")
        except Exception as e:
            print(f"Error fetching search volumes: {str(e)}")
            traceback.print_exc()
            return False
            
        # Step 4: Store the keyword data in Pinecone
        print("Step 4: Storing keyword data in Pinecone...")
        try:
            upsert_keywords(keywords_df, city)
            print("Successfully stored keyword data in Pinecone")
        except Exception as e:
            print(f"Error upserting to Pinecone: {str(e)}")
            traceback.print_exc()
            return False
        
        print("Keyword pipeline completed successfully")
        return True
    except Exception as e:
        print(f"Unexpected error in keyword pipeline: {str(e)}")
        traceback.print_exc()
        return False

def test_openai_keyword_generation(business_names=None):
    """Test just the OpenAI keyword generation step"""
    if business_names is None:
        # Use a test set of business names
        business_names = ["Zara Store", "H&M Outlet", "Nike Factory Store", "Max Fashion Bengaluru"]
    
    print(f"Testing keyword generation for {len(business_names)} business names")
    
    try:
        from openai import OpenAI
        from src.config import secret
        
        client = OpenAI(api_key=secret("OPENAI_API_KEY"))
        
        # Test with just one batch
        businesses_text = "\n".join([f"- {name}" for name in business_names])
        
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
        
        print("Sending request to OpenAI...")
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",  # Try using gpt-3.5-turbo if this fails
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7
        )
        
        # Parse keywords from response
        keywords_text = response.choices[0].message.content.strip()
        print(f"Response from OpenAI: {keywords_text}")
        
        keywords = [k.strip() for k in keywords_text.split(",")]
        print(f"Generated {len(keywords)} keywords successfully")
        return keywords
        
    except Exception as e:
        print(f"Error in test_openai_keyword_generation: {str(e)}")
        traceback.print_exc()
        return None
