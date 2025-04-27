"""
openai_assistant_reporting.py - Generate advanced reports using OpenAI Assistant with combined data
"""
import os
import time
import json
import pandas as pd
import streamlit as st
from typing import Dict, Any, List, Optional
from openai import OpenAI
from src.config import secret

class AssistantReporter:
    """
    Class to handle OpenAI Assistant reporting with combined Pinecone data
    """
    
    def __init__(self):
        """Initialize the AssistantReporter"""
        self.client = OpenAI(api_key=secret("OPENAI_API_KEY"))
        self.assistant_id = self._get_or_create_assistant()
    
    def _get_or_create_assistant(self) -> str:
        """
        Get existing assistant or create a new one
        
        Returns:
            Assistant ID
        """
        # Try to get from session state first
        if "assistant_id" in st.session_state:
            return st.session_state.assistant_id
        
        # Look for existing assistants
        assistants = self.client.beta.assistants.list(limit=100)
        
        # Find assistant with the right name
        for assistant in assistants.data:
            if assistant.name == "ZeCompete Business Analyzer":
                # Store and return the existing assistant ID
                st.session_state.assistant_id = assistant.id
                return assistant.id
        
        # Create a new assistant if none found
        try:
            assistant = self.client.beta.assistants.create(
                name="ZeCompete Business Analyzer",
                instructions="""
                You are a specialized business and keyword analysis assistant for ZeCompete.
                
                Your role is to analyze business location data and keyword search volume data to provide 
                insights and recommendations.
                
                You will be provided with combined data from Pinecone containing:
                1. Business location data (name, address, rating, etc.) from the 'maps' namespace
                2. Keyword data (search volume, competition, CPC) from the 'keywords' namespace
                
                For each analysis, generate a comprehensive report that includes:
                1. Executive Summary
                2. Business Location Analysis
                3. Keyword Search Volume Analysis (including 12-month trends)
                4. Competitive Landscape
                5. Actionable Recommendations
                
                Include charts and visualizations in your report whenever possible.
                Use markdown formatting to make your report well-structured and readable.
                """,
                model="gpt-4o",
                tools=[{"type": "file_search"}]
            )
            
            # Store and return the new assistant ID
            st.session_state.assistant_id = assistant.id
            return assistant.id
        
        except Exception as e:
            st.error(f"Error creating assistant: {str(e)}")
            return None
    
    def upload_file(self, file_content: str, file_name: str = "combined_data.json") -> Optional[str]:
        """
        Upload a file to the OpenAI API
        
        Args:
            file_content: The content of the file as a string
            file_name: The name of the file
            
        Returns:
            File ID if successful, None otherwise
        """
        try:
            # Convert string to bytes
            file_bytes = file_content.encode('utf-8')
            
            # Create a temporary file
            temp_file_path = f"temp_{file_name}"
            with open(temp_file_path, "wb") as f:
                f.write(file_bytes)
            
            # Upload the file to OpenAI
            with open(temp_file_path, "rb") as f:
                file = self.client.files.create(
                    file=f,
                    purpose="assistants"
                )
            
            # Clean up temporary file
            os.remove(temp_file_path)
            
            # Return the file ID
            return file.id
            
        except Exception as e:
            st.error(f"Error uploading file: {str(e)}")
            return None
    
    def attach_file_to_assistant(self, file_id: str) -> bool:
        """
        Attach a file to the assistant
        
        Args:
            file_id: The ID of the file to attach
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.client.beta.assistants.files.create(
                assistant_id=self.assistant_id,
                file_id=file_id
            )
            return True
        except Exception as e:
            st.error(f"Error attaching file to assistant: {str(e)}")
            return False
    
    def generate_report(self, combined_data: Dict[str, Any], query: str) -> str:
        """
        Generate a report using the OpenAI Assistant with combined data
        
        Args:
            combined_data: Combined data from Pinecone (businesses and keywords)
            query: The user query to guide the analysis
            
        Returns:
            Generated report as markdown text
        """
        try:
            # Convert data to JSON string
            json_data = json.dumps(combined_data, indent=2)
            
            # Upload the data as a file
            file_id = self.upload_file(json_data)
            
            if not file_id:
                return "Error: Failed to upload data file."
            
            # Attach the file to the assistant
            if not self.attach_file_to_assistant(file_id):
                return "Error: Failed to attach file to assistant."
            
            # Create a thread
            thread = self.client.beta.threads.create()
            
            # Add a message to the thread
            self.client.beta.threads.messages.create(
                thread_id=thread.id,
                role="user",
                content=f"""
                Please analyze the attached combined data file with business and keyword information.
                
                User Query: {query}
                
                Generate a comprehensive report with the following sections:
                1. Executive Summary
                2. Business Location Analysis
                3. Keyword Search Volume Analysis (with 12-month trends)
                4. Competitive Landscape
                5. Actionable Recommendations
                
                Include insights on search volume trends, competition metrics, and business performance.
                Format your response in markdown for readability.
                """
            )
            
            # Run the assistant
            run = self.client.beta.threads.runs.create(
                thread_id=thread.id,
                assistant_id=self.assistant_id
            )
            
            # Poll for completion
            while run.status in ["queued", "in_progress"]:
                # Check status
                run = self.client.beta.threads.runs.retrieve(
                    thread_id=thread.id,
                    run_id=run.id
                )
                
                # Wait a bit before polling again
                time.sleep(2)
            
            # Check if the run completed successfully
            if run.status != "completed":
                return f"Error: Assistant run failed with status: {run.status}"
            
            # Get the assistant's response
            messages = self.client.beta.threads.messages.list(
                thread_id=thread.id
            )
            
            # Get the latest assistant message
            for message in messages.data:
                if message.role == "assistant":
                    # Extract the content
                    content_parts = []
                    for content in message.content:
                        if content.type == "text":
                            content_parts.append(content.text.value)
                    
                    # Join all content parts
                    report = "\n\n".join(content_parts)
                    return report
            
            return "Error: No response from assistant."
            
        except Exception as e:
            return f"Error generating report: {str(e)}"
    
    def list_assistant_files(self) -> List[Dict[str, Any]]:
        """
        List all files attached to the assistant
        
        Returns:
            List of file information dictionaries
        """
        try:
            files = self.client.beta.assistants.files.list(
                assistant_id=self.assistant_id
            )
            return [{"id": file.id, "created_at": file.created_at} for file in files.data]
        except Exception as e:
            st.error(f"Error listing assistant files: {str(e)}")
            return []
            
    def delete_file(self, file_id: str) -> bool:
        """
        Delete a file from the assistant and the API
        
        Args:
            file_id: The ID of the file to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # First remove from assistant
            self.client.beta.assistants.files.delete(
                assistant_id=self.assistant_id,
                file_id=file_id
            )
            
            # Then delete from API
            self.client.files.delete(file_id=file_id)
            
            return True
        except Exception as e:
            st.error(f"Error deleting file: {str(e)}")
            return False

def render_assistant_report_tab():
    """
    Render the OpenAI Assistant reporting tab in Streamlit
    """
    st.header("Advanced Business & Keyword Reporting")
    
    # Initialize the reporter
    reporter = AssistantReporter()
    
    # Show some information about the assistant
    with st.expander("Assistant Information"):
        st.write(f"Assistant ID: {reporter.assistant_id}")
        
        # List attached files
        files = reporter.list_assistant_files()
        if files:
            st.write(f"Files attached to assistant: {len(files)}")
            for file in files:
                st.write(f"- File ID: {file['id']} (Created: {file['created_at']})")
                if st.button(f"Delete File {file['id'][:8]}...", key=f"delete_{file['id']}"):
                    if reporter.delete_file(file['id']):
                        st.success(f"File {file['id']} deleted successfully")
                        st.rerun()
                    else:
                        st.error(f"Failed to delete file {file['id']}")
        else:
            st.write("No files attached to assistant")
    
    # Input section
    st.subheader("Generate Advanced Report")
    
    # Option 1: Upload JSON file
    uploaded_file = st.file_uploader("Upload combined data JSON file", type=["json"])
    
    # Option 2: Use existing data in Pinecone
    use_existing = st.checkbox("Use existing data from Pinecone instead of uploading")
    
    # Query input
    query = st.text_area(
        "Specific analysis request",
        "Analyze the business locations and keyword search trends. Identify the top businesses by search interest and provide recommendations for optimization."
    )
    
    # Generate report button
    if st.button("🚀 Generate Comprehensive Report"):
        with st.spinner("Generating advanced report with OpenAI Assistant..."):
            try:
                combined_data = None
                
                # Get data from uploaded file or Pinecone
                if uploaded_file is not None:
                    # Read from uploaded file
                    combined_data = json.loads(uploaded_file.getvalue().decode("utf-8"))
                    st.success("Successfully loaded data from uploaded file")
                elif use_existing:
                    # Import the function to get combined data
                    from enhanced_keyword_pipeline import combine_data_for_assistant
                    
                    # Get data from Pinecone
                    combined_data = combine_data_for_assistant(query)
                    if "error" in combined_data:
                        st.error(f"Error getting data from Pinecone: {combined_data['error']}")
                        combined_data = None
                    else:
                        st.success("Successfully retrieved data from Pinecone")
                
                if combined_data:
                    # Generate the report
                    report = reporter.generate_report(combined_data, query)
                    
                    # Display the report
                    st.subheader("Generated Report")
                    st.markdown(report)
                    
                    # Download option
                    st.download_button(
                        label="⬇️ Download Report as Markdown",
                        data=report.encode("utf-8"),
                        file_name=f"business_keyword_report_{time.strftime('%Y%m%d_%H%M%S')}.md",
                        mime="text/markdown"
                    )
                else:
                    st.warning("Please upload a JSON file or use existing data from Pinecone")
            except Exception as e:
                st.error(f"Error generating report: {str(e)}")
                import traceback
                st.code(traceback.format_exc())

if __name__ == "__main__":
    # Run as standalone app for testing
    st.set_page_config(page_title="Advanced Reporting", layout="wide")
    render_assistant_report_tab()
