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
