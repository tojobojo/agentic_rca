from agents import Agent
import asyncio

class Runner:
    @staticmethod
    async def run(agent: Agent, prompt: str):
        """Executes the agent asynchronously."""
        # Assuming the 'agents' library has an async run method. 
        # If not, we might need to wrap a sync call.
        # Common pattern: agent.run() or agent.arun()
        # Let's try .run_async() or .acall()
        # Given I cannot verify the library version, I will implement a safe wrapper.
        
        try:
            if hasattr(agent, "run_async"):
                return await agent.run_async(prompt)
            elif hasattr(agent, "arun"):
                return await agent.arun(prompt)
            else:
                 # Fallback to sync run in thread if no async method
                 return await asyncio.to_thread(agent.run, prompt)
        except Exception as e:
            raise RuntimeError(f"Runner failed: {e}")
