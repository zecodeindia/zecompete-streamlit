# src/analytics.py - Updated to handle both business and keyword data
from pinecone import Pinecone
from src.config import secret
from openai import OpenAI

# Updated Pinecone initialization
pc = Pinecone(api_key=secret("PINECONE_API_KEY"))
INDEX_NAME = "zecompete"
index = pc.Index(INDEX_NAME)

# Initialize OpenAI client
client = OpenAI(api_key=secret("OPENAI_API_KEY"))

def insight_question(question: str) -> str:
    """
    Ask a question grounded in your Pinecone data.
    Simple implementation without langchain.
    """
    try:
        # Create an embedding for the question
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=[question]
        )
        query_embedding = response.data[0].embedding
        
        # Query both maps and keywords namespaces
        map_contexts = []
        keyword_contexts = []
        
        # Try maps namespace first
        try:
            results = index.query(
                vector=query_embedding,
                top_k=8,
                namespace="maps",
                include_metadata=True
            )
            map_contexts = [
                f"Business: {match.metadata.get('name', '')}, "
                f"Location: {match.metadata.get('city', '')}, "
                f"Rating: {match.metadata.get('rating', 'N/A')}"
                for match in results.matches if match.metadata
            ]
        except Exception as e:
            print(f"Error querying maps namespace: {str(e)}")
        
        # Then try keywords namespace
        try:
            results = index.query(
                vector=query_embedding,
                top_k=8,
                namespace="keywords",
                include_metadata=True
            )
            keyword_contexts = [
                f"Keyword: {match.metadata.get('keyword', '')}, "
                f"Search Volume: {match.metadata.get('search_volume', 'N/A')}, "
                f"Period: {match.metadata.get('month', '')}/{match.metadata.get('year', '')}"
                for match in results.matches if match.metadata
            ]
        except Exception as e:
            print(f"Error querying keywords namespace: {str(e)}")
        
        # Combine contexts with appropriate labels
        contexts = []
        if map_contexts:
            contexts.append("BUSINESS DATA:")
            contexts.extend(map_contexts)
        
        if keyword_contexts:
            if contexts:  # Add a separator if we already have business data
                contexts.append("\n")
            contexts.append("KEYWORD DATA:")
            contexts.extend(keyword_contexts)
        
        # If we have contexts, use them to ground the answer
        if contexts:
            context_text = "\n".join([f"- {ctx}" for ctx in contexts if ctx])
            prompt = f"""
            Based on the following information:
            {context_text}
            
            Please answer this question: {question}
            
            If the question is about search trends or keyword popularity, focus on the KEYWORD DATA.
            If the question is about business locations or ratings, focus on the BUSINESS DATA.
            
            If the information provided doesn't address the question directly, 
            please say so and answer based only on what is available.
            """
            
            chat_response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2
            )
            return chat_response.choices[0].message.content
        else:
            return "I don't have enough information in the database to answer that question."
    
    except Exception as e:
        return f"Sorry, I encountered an error when trying to answer your question: {str(e)}"
