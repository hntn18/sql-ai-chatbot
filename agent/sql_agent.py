from langchain import hub
import langchain
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langgraph.prebuilt import create_react_agent
from langchain.agents import create_agent
from langchain_core.messages.ai import AIMessage
from langchain_core.messages.human import HumanMessage

from services.azure_services import AzureServices
from config.config import settings

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
class SQLAgent:

    def __init__(self):        
        """
        Initializes the SQLAgent with database connection and LLM model.
        """
        # Load Azure OpenAI model
        self.azure_services = AzureServices()
        self.model = self.azure_services.model

        # Database configuration
        self.database_uri = (
            f"postgresql://{settings.DATABASE_USERNAME}:"
            f"{settings.DATABASE_PASSWORD}@{settings.DATABASE_HOST}:"
            f"{settings.DATABASE_PORT}/{settings.DATABASE}"
        )
        self.db = SQLDatabase.from_uri(self.database_uri)
        self.toolkit = SQLDatabaseToolkit(db=self.db, llm=self.model)

        # Prepare agent
        self.sql_tools = self.toolkit.get_tools()
        prompt_template = hub.pull("langchain-ai/sql-agent-system-prompt")
        # You could make dialect and top_k configurable if needed
        self.system_prompt = """
            You are an agent designed to interact with a SQL database.
            Given an input question, create a syntactically correct {dialect} query to run,
            then look at the results of the query and return the answer. Unless the user
            specifies a specific number of examples they wish to obtain, always limit your
            query to at most {top_k} results.

            You can order the results by a relevant column to return the most interesting
            examples in the database. Never query for all the columns from a specific table,
            only ask for the relevant columns given the question.

            You MUST double check your query before executing it. If you get an error while
            executing a query, rewrite the query and try again.

            DO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the
            database.

            To start you should ALWAYS look at the tables in the database to see what you
            can query. Do NOT skip this step.

            Then you should query the schema of the most relevant tables.
            """.format(
                dialect=self.db.dialect,
                top_k=5,
            )
        # self.system_message = prompt_template.format(dialect="PostgreSQL", top_k=5)
        self.agent_executor = create_react_agent(
            self.model,
            self.sql_tools,
            prompt=self.system_prompt
        )

    def run_query(self, user_query) -> AIMessage:
        """
        Executes the SQL agent with the provided user query and prints results.
        :param user_query: The SQL query in natural language.
        """
        # events = self.agent_executor.invoke(
        #     {"messages": [("user", user_query)]},
        #     stream_mode="values",
        # )
        # for event in events["messages"]:
        #     event.pretty_print()
        # events = self.agent_executor.invoke(
        #     {"messages": [user_query]},
        #     stream_mode="values",
        # )
        events = self.agent_executor.invoke(
            user_query,
            stream_mode="values",
        )
        for event in events["messages"]:
            logger.info(event.pretty_print())
        # Get the last message
        last_message = events["messages"][-1]
        if isinstance(last_message, AIMessage):
            return last_message
        else:
            raise ValueError("Last message is not an AIMessage. Got: {}".format(type(last_message)))
    
# Usage Example:
if __name__ == "__main__":
    sql_agent = SQLAgent()
    user_query = input("Enter your question: ")
    sql_agent.run_query(user_query)