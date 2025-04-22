# src/analytics.py - Fixed version
from pinecone import Pinecone
from src.config import secret
from langchain_pinecone import PineconeVectorStore
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough

# Initialize Pinecone
pc = Pinecone(api_key=secret("PINECONE_API_KEY"))

# Prepare your embedding object
embedding = OpenAIEmbeddings(
    model="text-embedding-3-small",
    openai_api_key=secret("OPENAI_API_KEY")
)

# LLM setup
llm = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0.2,
    openai_api_key=secret("OPENAI_API_KEY")
)

def insight_question(question: str) -> str:
    """
    Ask a question grounded in your Pinecone data.
    Uses updated LangChain patterns for better reliability.
    """
    try:
        # Determine which namespace to query based on the question
        namespace = "keywords" if any(kw in question.lower() for kw in 
                                    ["keyword", "search", "volume", "trend"]) else "maps"
        
        # Create vector store with appropriate namespace
        vector_store = PineconeVectorStore.from_existing_index(
            index_name="zecompete",
            embedding=embedding,
            namespace=namespace
        )
        
        # Create the retriever
        retriever = vector_store.as_retriever(search_kwargs={"k": 5})
        
        # Define prompt template
        template = """Answer the following question based on the provided context.
        If the information is not in the context, say you don't have enough information.
        
        Context: {context}
        
        Question: {question}
        
        Answer: """
        
        prompt = ChatPromptTemplate.from_template(template)
        
        # Create the RAG chain using the LCEL (LangChain Expression Language) pattern
        # This is the updated way to create chains in LangChain
        chain = (
            {"context": retriever, "question": RunnablePassthrough()}
            | prompt
            | llm
            | StrOutputParser()
        )
        
        # Execute the chain
        return chain.invoke(question)
    
    except Exception as e:
        # Return a helpful error message
        return f"Sorry, I encountered an error: {str(e)}. Please check if there's data in the index or try a different question."