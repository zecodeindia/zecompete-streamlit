# src/analytics.py - Updated with new Pinecone initialization
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
        
        # Query both namespaces
        try:
            # Try maps namespace first
            results = index.query(
                vector=query_embedding,
                top_k=8,
                namespace="maps",
                include_metadata=True
            )
            contexts = [match.metadata.get("name", "") for match in results.matches if match.metadata]
        except:
            try:
                # Try keywords namespace if maps fails
                results = index.query(
                    vector=query_embedding,
                    top_k=8,
                    namespace="keywords",
                    include_metadata=True
                )
                contexts = [match.metadata.get("keyword", "") for match in results.matches if match.metadata]
            except:
                contexts = []
        
        # If we have contexts, use them to ground the answer
        if contexts:
            context_text = "\n".join([f"- {ctx}" for ctx in contexts if ctx])
            prompt = f"""
            Based on the following information:
            {context_text}
            
            Please answer this question: {question}
            
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
