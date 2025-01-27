import os
import streamlit as st
from dotenv import load_dotenv
import assemblyai as aai
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain_core.messages import HumanMessage
import requests
import time
from urllib.parse import urlparse
from pathlib import Path
import gdown
import re
from typing import List

# Load environment variables
load_dotenv()

# Initialize API keys
ASSEMBLYAI_API_KEY = os.getenv("ASSEMBLYAI_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Initialize clients
aai.settings.api_key = ASSEMBLYAI_API_KEY
llm = ChatOpenAI(api_key=OPENAI_API_KEY)


# Define analysis prompts
# Defines templates for different types of analysis that can be performed on the transcription.
ANALYSIS_PROMPTS = {
    "summary": """
    Provide a concise summary of the following transcription:
    {transcription}
    
    Focus on:
    - Main topics discussed
    - Key points and takeaways
    - Important conclusions
    - Highlight critical decisions or agreements made
    """,
    
    "objections": """
    Analyze the following transcription and identify customer objections:
    {transcription}
    
    Please provide:
    - List each objection with exact customer quotes
    - Context around each objection
    - Severity or importance of each objection
    - How the objection was addressed (if it was)
    
    Format each objection as:
    1. Quote: "[exact customer words]"
       Context: [explain the situation]
       Severity: [High/Medium/Low]
       Resolution: [how it was addressed]
    """,
    
    "customer_sentiments": """
    Analyze the customer's emotional state and sentiment throughout the conversation:
    {transcription}
    
    Provide:
    - Overall sentiment (positive, negative, neutral, mixed)
    - Emotional progression throughout the conversation
    - Key moments where sentiment shifted
    - Supporting quotes for each sentiment observation
    
    Format as:
    1. Sentiment: [description]
       Quote: "[exact customer words]"
       Context: [explain what led to this sentiment]
    """,
    
    "follow_up_tasks": """
    Extract all action items, commitments, and follow-up tasks from the conversation:
    {transcription}
    
    List:
    - Specific tasks or actions promised
    - Deadlines or timeframes mentioned
    - Responsibilities assigned
    - Any conditions or dependencies
    
    Format each task as:
    1. Task: [description]
       Quote: "[exact words from conversation]"
       Owner: [who is responsible]
       Timeline: [when it needs to be done]
    """,
    
    "use_cases": """
    Identify all use cases and scenarios discussed in the conversation:
    {transcription}
    
    For each use case:
    - Quote the exact discussion
    - Describe the scenario
    - Note any specific requirements
    - Highlight customer's specific needs or pain points
    
    Format as:
    1. Use Case: [title]
       Quote: "[exact conversation excerpt]"
       Description: [detailed explanation]
       Requirements: [specific needs mentioned]
    """
}

def extract_gdrive_id(url: str) -> str:
    """Extract Google Drive file ID from URL"""
    # Handle different Google Drive URL formats
    patterns = [
        r'https://drive\.google\.com/file/d/(.*?)/',  # Direct file link
        r'https://drive\.google\.com/open\?id=(.*?)(?:&|$)',  # Open link
        r'https://drive\.google\.com/drive/folders/(.*?)(?:/|$)'  # Folder link
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return url  # Return original URL if no pattern matches

def get_onedrive_content(url: str) -> bytes:
    """Get file content from OneDrive/SharePoint shared URL"""
    try:
        # Clean up the URL
        base_url = url.split('?')[0]
        download_url = f"{base_url}?download=1"
        
        # Make the request with appropriate headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }
        
        # First try to get the download URL
        session = requests.Session()
        response = session.get(download_url, headers=headers, allow_redirects=True)
        response.raise_for_status()
        
        return response.content
            
    except Exception as e:
        st.error("""
        Error accessing SharePoint file. Please make sure:
        1. The link is a direct download link (ends with ?download=1)
        2. The file is shared with "Anyone with the link can view"
        3. You've removed any extra parameters from the URL
        """)
        raise Exception(f"OneDrive access error: {str(e)}")

# Central function for handling all file types
# Supports local files, OneDrive, Google Drive, and other URLs
# Creates temporary files and manages cleanup

def download_file(path_or_url: str) -> str:
    """Handle local files, URLs, and OneDrive links"""
    output_path = None
    try:
        # Create temp directory if it doesn't exist
        temp_dir = Path("temp_files")
        temp_dir.mkdir(exist_ok=True)
                 
        # Check if input is a local path
        input_path = Path(path_or_url)
        if input_path.exists():
            # It's a local file
            extension = input_path.suffix
            output_path = temp_dir / f"temp_audio_{int(time.time())}{extension}"
            # Copy the file to temp directory
            import shutil
            shutil.copy2(input_path, output_path)
            return str(output_path)
            
        # Generate a default output path
        output_path = temp_dir / f"temp_audio_{int(time.time())}"
            
        # Check if it's a OneDrive/SharePoint URL
        if 'sharepoint.com' in path_or_url or 'onedrive.com' in path_or_url:
            # Get content from OneDrive
            content = get_onedrive_content(path_or_url)
            
            # Determine file extension from URL
            extension = Path(urlparse(path_or_url).path).suffix or '.mp4'
            output_path = output_path.with_suffix(extension)
            
            # Save the content
            with open(output_path, 'wb') as f:
                f.write(content)
                
        elif 'drive.google.com' in path_or_url:
            file_id = extract_gdrive_id(path_or_url)
            if 'folders' in path_or_url:
                # It's a folder
                output_path = temp_dir
                gdown.download_folder(url=path_or_url, output=str(output_path), quiet=False)
                # Get the first audio/video file in the folder
                for file in Path(output_path).glob('*'):
                    if file.suffix.lower() in ['.mp3', '.mp4', '.wav', '.m4a']:
                        return str(file)
                raise Exception("No audio/video file found in the folder")
            else:
                # It's a single file
                output_path = output_path.with_suffix('.mp4')  # Default to mp4
                gdown.download(id=file_id, output=str(output_path), quiet=False)
        else:
            # Regular download for other URLs
            response = requests.get(path_or_url, stream=True)
            response.raise_for_status()
            
            # Get content type and set appropriate extension
            content_type = response.headers.get('content-type', '').lower()
            if 'audio' in content_type or 'video' in content_type:
                extension = content_type.split('/')[-1]
            else:
                extension = 'mp3'
            
            output_path = output_path.with_suffix(f".{extension}")
            
            # Save the file
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                    
        return str(output_path)
        
    except Exception as e:
        if output_path and Path(output_path).exists():
            if Path(output_path).is_file():
                Path(output_path).unlink()
            elif Path(output_path).is_dir():
                import shutil
                shutil.rmtree(output_path)
        raise Exception(f"Download error: {str(e)}")

# Transcribe Audio/Video using AssemblyAI
# Supports speaker identification

def transcribe_video(url: str, enable_speaker_id: bool = True) -> str:
    """
    Transcribe audio/video using AssemblyAI with optional speaker identification
    """
    try:
        # First download the file
        audio_file = download_file(url)
        
        # Verify file exists and has content
        if not Path(audio_file).exists():
            raise Exception(f"Audio file not found: {audio_file}")
            
        file_size = Path(audio_file).stat().st_size
        if file_size == 0:
            raise Exception("Audio file is empty")
            
        st.info(f"Successfully downloaded file. Size: {file_size/1024/1024:.2f} MB")
        
        # Create the transcriber with speaker diarization if enabled
        config = aai.TranscriptionConfig(
            speaker_labels=enable_speaker_id,
            speakers_expected=2 if enable_speaker_id else None
        )
        
        transcriber = aai.Transcriber()
        
        # Transcribe with error checking
        with open(audio_file, 'rb') as f:
            st.info("Starting transcription...")
            transcript = transcriber.transcribe(f, config=config)
            
        if transcript is None:
            raise Exception("Transcription failed - no result returned")
            
        # Add debug info
        st.info(f"Transcription status: {transcript.status}")
        
        if not hasattr(transcript, 'text') or not transcript.text:
            if hasattr(transcript, 'error'):
                raise Exception(f"Transcription error: {transcript.error}")
            raise Exception("Transcription completed but no text was generated")
        
        # Format transcript with speaker labels if enabled
        formatted_transcript = ""
        
        # Check if utterances are available and speaker identification was enabled
        if enable_speaker_id and hasattr(transcript, 'utterances') and transcript.utterances:
            for utterance in transcript.utterances:
                formatted_transcript += f"Speaker {utterance.speaker}: {utterance.text}\n\n"
        else:
            formatted_transcript = transcript.text
            
        if not formatted_transcript:
            raise Exception("No transcription text was generated")
        
        # Clean up the temporary file
        Path(audio_file).unlink()
        
        return formatted_transcript
        
    except Exception as e:
        # Clean up in case of error
        if 'audio_file' in locals() and Path(audio_file).exists():
            Path(audio_file).unlink()
        raise Exception(f"Transcription error: {str(e)}")

# Split Transcription into Chunks
# This is to handle the issue of OpenAI's token limit

def split_into_chunks(text: str, max_tokens: int = 8000) -> List[str]:
    """
    Split text into chunks based on speaker segments
    """
    chunks = []
    current_chunk = ""
    current_length = 0
    
    # Split by speaker segments (double newline)
    segments = text.split('\n\n')
    
    for segment in segments:
        # Rough estimate of tokens (words * 1.3)
        segment_tokens = len(segment.split()) * 1.3
        
        if current_length + segment_tokens > max_tokens:
            # Current chunk is full
            if current_chunk:
                chunks.append(current_chunk.strip())
            current_chunk = segment
            current_length = segment_tokens
        else:
            # Add to current chunk
            current_chunk += "\n\n" + segment if current_chunk else segment
            current_length += segment_tokens
    
    # Add the last chunk
    if current_chunk:
        chunks.append(current_chunk.strip())
    
    return chunks

# Analyze Transcription Content
# This function uses chunking to handle the issue of OpenAI's token limit
# It combines the analyses of multiple chunks into a single response

def analyze_content(transcription: str, analysis_type: str) -> str:
    """Analyze transcription content based on selected type using chunking"""
    try:
        # Split transcription into chunks
        chunks = split_into_chunks(transcription)
        
        # Analyze each chunk
        chunk_analyses = []
        for i, chunk in enumerate(chunks):
            # Get appropriate prompt template
            if len(chunks) > 1:
                # Modify prompt for chunks
                chunk_prompt = f"""
                This is part {i+1} of {len(chunks)} of the transcription.
                {ANALYSIS_PROMPTS[analysis_type]}
                """
            else:
                chunk_prompt = ANALYSIS_PROMPTS[analysis_type]
                
            prompt_template = PromptTemplate(
                template=chunk_prompt,
                input_variables=["transcription"]
            )
            
            # Format prompt with chunk
            formatted_prompt = prompt_template.format(transcription=chunk)
            
            # Get response from OpenAI
            response = llm.invoke([HumanMessage(content=formatted_prompt)])
            chunk_analyses.append(response.content)
        
        # If there's only one chunk, return its analysis
        if len(chunk_analyses) == 1:
            return chunk_analyses[0]
        
        # If multiple chunks, combine them with a summary
        combined_analyses = "\n\n".join([
            f"Part {i+1} Analysis:\n{analysis}" 
            for i, analysis in enumerate(chunk_analyses)
        ])
        
        summary_prompt = PromptTemplate(
            template="""
            Below are analyses of different parts of a longer transcription. 
            Please combine these analyses into a single coherent response:
            
            {transcription}
            
            Provide a unified analysis that:
            1. Synthesizes the key points from all parts
            2. Maintains the original analysis structure
            3. Eliminates redundancy
            4. Presents a clear and organized final analysis
            """,
            input_variables=["transcription"]
        )
        
        final_response = llm.invoke([
            HumanMessage(content=summary_prompt.format(transcription=combined_analyses))
        ])
        
        return final_response.content
        
    except Exception as e:
        raise Exception(f"Analysis error: {str(e)}")

def create_streamlit_app():
    st.title("Audio/Video Analysis Application")
    
    # Initialize session state for transcription
    # Manages caching of transcriptions to avoid reprocessing.
    if 'transcription' not in st.session_state:
        st.session_state.transcription = None
        st.session_state.current_file = None
        st.session_state.speaker_id_enabled = None
    
    # Input method selection
    input_method = st.radio(
        "Choose input method:",
        ["File Upload", "SharePoint/OneDrive", "Other URL"]
    )
    
    file_path = None
    
    if input_method == "File Upload":
        uploaded_file = st.file_uploader("Upload an audio/video file", 
                                       type=['mp3', 'mp4', 'wav', 'm4a'])
        if uploaded_file:
            # Save uploaded file temporarily
            temp_dir = Path("temp_files")
            temp_dir.mkdir(exist_ok=True)
            file_path = temp_dir / uploaded_file.name
            with open(file_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
    elif input_method == "SharePoint/OneDrive":
        st.info("""
        For SharePoint/OneDrive files:
        1. Open the file in SharePoint/OneDrive
        2. Click 'Share'
        3. Click 'Anyone with the link can view'
        4. Click 'Copy Link'
        5. Important: After copying the link:
           - Remove everything after '?' in the URL
           - Add '?download=1' at the end
        """)
        file_path = st.text_input("Enter SharePoint/OneDrive direct download link:")
        
        if file_path and '?' in file_path:
            base_url = file_path.split('?')[0]
            modified_url = f"{base_url}?download=1"
            st.info(f"Modified URL (use this instead):\n{modified_url}")
            file_path = modified_url
    else:
        file_path = st.text_input("Enter File URL:")
    
    # Add speaker identification checkbox
    enable_speaker_id = st.checkbox("Enable Speaker Identification", value=True,
                                  help="Identify different speakers in the conversation")
    
    analysis_type = st.selectbox(
        "Select Analysis Type",
        ["summary", "objections", "customer_sentiments", "follow_up_tasks", "use_cases"],
        format_func=lambda x: {
            "summary": "Summary",
            "objections": "Objections",
            "customer_sentiments": "Customer Sentiments",
            "follow_up_tasks": "Follow-Up Tasks",
            "use_cases": "Use Cases Discussed"
        }[x]
    )
    
    if st.button("Process File"):
        if file_path:
            try:
                # Check if we need to transcribe or can use cached transcription
                if (st.session_state.transcription is None or 
                    st.session_state.current_file != str(file_path) or
                    st.session_state.speaker_id_enabled != enable_speaker_id):
                    
                    with st.spinner("Transcribing audio..."):
                        st.info(f"Processing file: {file_path}")
                        transcription = transcribe_video(str(file_path), enable_speaker_id)
                        
                        if not transcription:
                            st.error("Transcription returned empty result")
                            return
                            
                        # Cache the transcription, file path, and speaker ID setting
                        st.session_state.transcription = transcription
                        st.session_state.current_file = str(file_path)
                        st.session_state.speaker_id_enabled = enable_speaker_id
                        
                        st.success("Transcription completed!")
                else:
                    st.info("Using cached transcription")
                    transcription = st.session_state.transcription
                
                # Always show transcription
                st.text_area("Transcription", transcription, height=200)
                
                # Analyze content
                with st.spinner("Analyzing content..."):
                    analysis = analyze_content(transcription, analysis_type)
                    st.success("Analysis completed!")
                    st.subheader(f"{analysis_type.title()} Analysis")
                    st.write(analysis)
                    
            except Exception as e:
                st.error(f"Error: {str(e)}")
                import traceback
                st.error(f"Detailed error: {traceback.format_exc()}")
            finally:
                # Clean up uploaded file if it exists
                if input_method == "File Upload" and file_path and file_path.exists():
                    file_path.unlink()
        else:
            st.warning("Please provide a file or URL")
    
    # Add a clear cache button
    if st.session_state.transcription is not None:
        if st.button("Clear Cached Transcription"):
            st.session_state.transcription = None
            st.session_state.current_file = None
            st.session_state.speaker_id_enabled = None
            st.success("Cache cleared! Next analysis will require new transcription.")

if __name__ == "__main__":
    create_streamlit_app()