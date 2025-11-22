from langchain_openai import AzureChatOpenAI, AzureOpenAIEmbeddings
import os 
from config.config import settings
import os

from langchain_community.chat_models import PaiEasChatEndpoint
from langchain_core.language_models.chat_models import HumanMessage

os.environ["EAS_SERVICE_URL"] = "Your_EAS_Service_URL"
os.environ["EAS_SERVICE_TOKEN"] = "Your_EAS_Service_Token"
chat = PaiEasChatEndpoint(
    eas_service_url=os.environ["EAS_SERVICE_URL"],
    eas_service_token=os.environ["EAS_SERVICE_TOKEN"],
)