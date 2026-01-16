"""
Voice Interface for PCA Agent
Speech-to-text and text-to-speech capabilities
"""

from typing import Optional, Dict, Any
import os
from loguru import logger

class VoiceInterface:
    """Voice interface for hands-free interaction."""
    
    def __init__(self):
        """Initialize voice interface."""
        self.stt_provider = os.getenv("STT_PROVIDER", "openai")  # openai, google, azure
        self.tts_provider = os.getenv("TTS_PROVIDER", "openai")
        
        logger.info(f"✅ Voice Interface initialized (STT: {self.stt_provider}, TTS: {self.tts_provider})")
    
    def speech_to_text(
        self,
        audio_file: str,
        language: str = "en"
    ) -> Dict[str, Any]:
        """
        Convert speech to text.
        
        Args:
            audio_file: Path to audio file
            language: Language code
        
        Returns:
            Transcription result
        """
        try:
            if self.stt_provider == "openai":
                return self._openai_stt(audio_file, language)
            elif self.stt_provider == "google":
                return self._google_stt(audio_file, language)
            elif self.stt_provider == "azure":
                return self._azure_stt(audio_file, language)
            else:
                raise ValueError(f"Unknown STT provider: {self.stt_provider}")
        
        except Exception as e:
            logger.error(f"Speech-to-text error: {e}")
            return {
                "success": False,
                "error": str(e),
                "text": ""
            }
    
    def text_to_speech(
        self,
        text: str,
        voice: str = "alloy",
        output_file: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Convert text to speech.
        
        Args:
            text: Text to convert
            voice: Voice to use
            output_file: Optional output file path
        
        Returns:
            TTS result
        """
        try:
            if self.tts_provider == "openai":
                return self._openai_tts(text, voice, output_file)
            elif self.tts_provider == "google":
                return self._google_tts(text, voice, output_file)
            elif self.tts_provider == "azure":
                return self._azure_tts(text, voice, output_file)
            else:
                raise ValueError(f"Unknown TTS provider: {self.tts_provider}")
        
        except Exception as e:
            logger.error(f"Text-to-speech error: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def _openai_stt(self, audio_file: str, language: str) -> Dict[str, Any]:
        """OpenAI Whisper speech-to-text."""
        try:
            from openai import OpenAI
            client = OpenAI()
            
            with open(audio_file, "rb") as audio:
                transcript = client.audio.transcriptions.create(
                    model="whisper-1",
                    file=audio,
                    language=language
                )
            
            return {
                "success": True,
                "text": transcript.text,
                "provider": "openai"
            }
        
        except Exception as e:
            logger.error(f"OpenAI STT error: {e}")
            return {"success": False, "error": str(e), "text": ""}
    
    def _openai_tts(
        self,
        text: str,
        voice: str,
        output_file: Optional[str]
    ) -> Dict[str, Any]:
        """OpenAI text-to-speech."""
        try:
            from openai import OpenAI
            client = OpenAI()
            
            response = client.audio.speech.create(
                model="tts-1",
                voice=voice,
                input=text
            )
            
            if output_file:
                response.stream_to_file(output_file)
                return {
                    "success": True,
                    "output_file": output_file,
                    "provider": "openai"
                }
            else:
                return {
                    "success": True,
                    "audio_data": response.content,
                    "provider": "openai"
                }
        
        except Exception as e:
            logger.error(f"OpenAI TTS error: {e}")
            return {"success": False, "error": str(e)}
    
    def _google_stt(self, audio_file: str, language: str) -> Dict[str, Any]:
        """Google Cloud speech-to-text."""
        try:
            from google.cloud import speech
            
            client = speech.SpeechClient()
            
            with open(audio_file, "rb") as audio:
                content = audio.read()
            
            audio = speech.RecognitionAudio(content=content)
            config = speech.RecognitionConfig(
                encoding=speech.RecognitionConfig.AudioEncoding.LINEAR16,
                language_code=language
            )
            
            response = client.recognize(config=config, audio=audio)
            
            text = " ".join([
                result.alternatives[0].transcript
                for result in response.results
            ])
            
            return {
                "success": True,
                "text": text,
                "provider": "google"
            }
        
        except Exception as e:
            logger.error(f"Google STT error: {e}")
            return {"success": False, "error": str(e), "text": ""}
    
    def _google_tts(
        self,
        text: str,
        voice: str,
        output_file: Optional[str]
    ) -> Dict[str, Any]:
        """Google Cloud text-to-speech."""
        try:
            from google.cloud import texttospeech
            
            client = texttospeech.TextToSpeechClient()
            
            synthesis_input = texttospeech.SynthesisInput(text=text)
            voice_params = texttospeech.VoiceSelectionParams(
                language_code="en-US",
                name=voice
            )
            audio_config = texttospeech.AudioConfig(
                audio_encoding=texttospeech.AudioEncoding.MP3
            )
            
            response = client.synthesize_speech(
                input=synthesis_input,
                voice=voice_params,
                audio_config=audio_config
            )
            
            if output_file:
                with open(output_file, "wb") as out:
                    out.write(response.audio_content)
                return {
                    "success": True,
                    "output_file": output_file,
                    "provider": "google"
                }
            else:
                return {
                    "success": True,
                    "audio_data": response.audio_content,
                    "provider": "google"
                }
        
        except Exception as e:
            logger.error(f"Google TTS error: {e}")
            return {"success": False, "error": str(e)}
    
    def _azure_stt(self, audio_file: str, language: str) -> Dict[str, Any]:
        """Azure speech-to-text."""
        try:
            import azure.cognitiveservices.speech as speechsdk
            
            speech_key = os.getenv("AZURE_SPEECH_KEY")
            service_region = os.getenv("AZURE_SPEECH_REGION")
            
            speech_config = speechsdk.SpeechConfig(
                subscription=speech_key,
                region=service_region
            )
            audio_config = speechsdk.AudioConfig(filename=audio_file)
            
            speech_recognizer = speechsdk.SpeechRecognizer(
                speech_config=speech_config,
                audio_config=audio_config
            )
            
            result = speech_recognizer.recognize_once()
            
            if result.reason == speechsdk.ResultReason.RecognizedSpeech:
                return {
                    "success": True,
                    "text": result.text,
                    "provider": "azure"
                }
            else:
                return {
                    "success": False,
                    "error": "Recognition failed",
                    "text": ""
                }
        
        except Exception as e:
            logger.error(f"Azure STT error: {e}")
            return {"success": False, "error": str(e), "text": ""}
    
    def _azure_tts(
        self,
        text: str,
        voice: str,
        output_file: Optional[str]
    ) -> Dict[str, Any]:
        """Azure text-to-speech."""
        try:
            import azure.cognitiveservices.speech as speechsdk
            
            speech_key = os.getenv("AZURE_SPEECH_KEY")
            service_region = os.getenv("AZURE_SPEECH_REGION")
            
            speech_config = speechsdk.SpeechConfig(
                subscription=speech_key,
                region=service_region
            )
            speech_config.speech_synthesis_voice_name = voice
            
            if output_file:
                audio_config = speechsdk.AudioConfig(filename=output_file)
                synthesizer = speechsdk.SpeechSynthesizer(
                    speech_config=speech_config,
                    audio_config=audio_config
                )
                synthesizer.speak_text(text)
                
                return {
                    "success": True,
                    "output_file": output_file,
                    "provider": "azure"
                }
            else:
                synthesizer = speechsdk.SpeechSynthesizer(
                    speech_config=speech_config
                )
                result = synthesizer.speak_text(text)
                
                return {
                    "success": True,
                    "audio_data": result.audio_data,
                    "provider": "azure"
                }
        
        except Exception as e:
            logger.error(f"Azure TTS error: {e}")
            return {"success": False, "error": str(e)}
    
    def process_voice_command(self, audio_file: str) -> Dict[str, Any]:
        """
        Process a voice command end-to-end.
        
        Args:
            audio_file: Path to audio file
        
        Returns:
            Command processing result
        """
        # Convert speech to text
        stt_result = self.speech_to_text(audio_file)
        
        if not stt_result["success"]:
            return {
                "success": False,
                "error": "Speech recognition failed",
                "details": stt_result
            }
        
        command_text = stt_result["text"]
        logger.info(f"Voice command: {command_text}")
        
        # Process command (integrate with your command processor)
        # For now, return the recognized text
        return {
            "success": True,
            "command": command_text,
            "stt_provider": stt_result["provider"]
        }


# Global instance
voice_interface = VoiceInterface()
