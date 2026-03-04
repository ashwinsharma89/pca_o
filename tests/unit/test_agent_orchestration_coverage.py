
import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock, ANY
from src.engine.agents.agent_registry import (
    AgentRegistry, AgentCapability, AgentRegistration, get_agent_registry, _registry
)
from src.engine.agents.multi_agent_orchestrator import (
    MultiAgentOrchestrator, AgentState, AgentResult, AgentType,
    RouterAgent, AnalyzerAgent, SpecialistAgent, RecommenderAgent, SynthesizerAgent,
    get_orchestrator, run_multi_agent_analysis
)

# --- Agent Registry Tests ---

class TestAgentRegistry:
    @pytest.fixture
    def registry(self):
        return AgentRegistry()

    def test_register_unregister(self, registry):
        # Register
        registry.register(
            name="test_agent",
            class_name="TestClass",
            module_path="test.module",
            capabilities=[AgentCapability.OCR],
            priority=5
        )
        assert "test_agent" in registry.agents
        assert "test_agent" in registry.capability_map[AgentCapability.OCR]
        
        # Duplicate register (log warning path)
        registry.register(
            name="test_agent",
            class_name="TestClass",
            module_path="test.module",
            capabilities=[AgentCapability.OCR]
        )
        assert registry.agents["test_agent"].priority == 0 # overwritten default
        
        # Unregister
        registry.unregister("test_agent")
        assert "test_agent" not in registry.agents
        assert "test_agent" not in registry.capability_map.get(AgentCapability.OCR, [])
        
        # Test unregister removing from capability list but list remains for other agents
        registry.register("a1", "C", "m", [AgentCapability.OCR])
        registry.register("a2", "C", "m", [AgentCapability.OCR])
        registry.unregister("a1")
        assert "a1" not in registry.capability_map[AgentCapability.OCR]
        assert "a2" in registry.capability_map[AgentCapability.OCR]
        
        # Test removing instance cache
        registry.register("a3", "C", "m", [AgentCapability.OCR])
        registry.instances["a3"] = "inst"
        registry.unregister("a3")
        assert "a3" not in registry.instances
        
        # Unregister invalid
        registry.unregister("non_existent") # Should not raise

    def test_post_init_validation(self):
        with pytest.raises(ValueError):
            AgentRegistration("n", "c", "m", [])

    def test_get_agent_lazy_loading(self, registry):
        registry.register(
            name="test_agent",
            class_name="TestAgent",
            module_path="src.test_module",
            capabilities=[AgentCapability.OCR]
        )
        
        # Mock importlib
        with patch("importlib.import_module") as mock_import:
            mock_class = MagicMock()
            mock_import.return_value.TestAgent = mock_class
            mock_class.return_value = "Instance"
            
            # First fetch (creates instance)
            agent = registry.get_agent("test_agent")
            assert agent == "Instance"
            mock_class.assert_called_once()
            
            # Second fetch (cached)
            agent2 = registry.get_agent("test_agent")
            assert agent2 == "Instance"
            # Should not call init again if no kwargs, but here we just check cache hit logic
            # The Registry implementation re-returns instance if exists and no init_kwargs
            assert registry.instances["test_agent"] == "Instance"

    def test_get_agent_failed_import(self, registry):
        registry.register(
            name="broken_agent",
            class_name="TestAgent",
            module_path="src.broken",
            capabilities=[AgentCapability.OCR]
        )
        with patch("importlib.import_module", side_effect=ImportError("Fail")):
            agent = registry.get_agent("broken_agent")
            assert agent is None
            assert registry.agents["broken_agent"].status == "unhealthy"

    def test_route_to_agent_priority(self, registry):
        registry.register(name="low_prio", class_name="C", module_path="m", capabilities=[AgentCapability.OCR], priority=1)
        registry.register(name="high_prio", class_name="C", module_path="m", capabilities=[AgentCapability.OCR], priority=10)
        
        # Mock get_agent to avoid import logic
        registry.instances["low_prio"] = "Low"
        registry.instances["high_prio"] = "High"
        
        # Should pick high prio first
        agent = registry.route_to_agent(AgentCapability.OCR)
        assert agent == "High"

    def test_health_check(self, registry):
        registry.register(name="agent", class_name="C", module_path="m", capabilities=[AgentCapability.OCR])
        
        # 1. Agent not created/loadable
        with patch("src.engine.agents.agent_registry.AgentRegistry.get_agent", return_value=None):
            assert registry.health_check("agent") is False
            assert registry.agents["agent"].status == "unhealthy"
            
        # 2. Agent loaded, no health_check method
        mock_agent = MagicMock(spec=[]) # No health_check
        registry.instances["agent"] = mock_agent
        assert registry.health_check("agent") is True
        assert registry.agents["agent"].status == "healthy"
        
        # 3. Agent loaded, with health_check method
        mock_agent.health_check = MagicMock(return_value=False)
        assert registry.health_check("agent") is False
        assert registry.agents["agent"].status == "unhealthy"

        # 4. Exception in health check
        mock_agent.health_check.side_effect = Exception("Boom")
        assert registry.health_check("agent") is False
        assert registry.agents["agent"].status == "unhealthy"

    def test_health_check_all(self, registry):
        registry.register("a1", "C", "m", [AgentCapability.OCR])
        mock_a1 = MagicMock()
        mock_a1.health_check.return_value = True
        registry.instances["a1"] = mock_a1
        res = registry.health_check_all()
        assert res["a1"] is True

    def test_health_check_unregistered(self, registry):
        assert registry.health_check("ghost") is False

    def test_get_agent_not_found(self, registry):
        assert registry.get_agent("missing") is None

    def test_get_agent_with_kwargs(self, registry):
        # Init kwargs should bypass cache or not cache? code says:
        # if name in self.instances and not init_kwargs: return instance
        # if not init_kwargs: self.instances[name] = instance
        
        registry.register("a", "Agent", "mod", [AgentCapability.OCR])
        with patch("importlib.import_module") as mock_imp:
            mock_imp.return_value.Agent.return_value = "Instance1"
            # 1. First call with kwargs
            a1 = registry.get_agent("a", key="val")
            assert a1 == "Instance1"
            assert "a" not in registry.instances # Not cached
            
            # 2. Second call with kwargs (new instance)
            mock_imp.return_value.Agent.return_value = "Instance2"
            a2 = registry.get_agent("a", key="val")
            assert a2 == "Instance2"
            
    def test_routing_edge_cases(self, registry):
        # 1. No agents for capability
        assert registry.route_to_agent(AgentCapability.OCR) is None
        
        # 2. Agents exist but get_agent returns None (e.g. import fail)
        registry.register("broken", "C", "m", [AgentCapability.OCR])
        with patch.object(registry, 'get_agent', return_value=None):
            assert registry.route_to_agent(AgentCapability.OCR) is None

    def test_find_agents_filtering(self, registry):
        # Empty capability
        assert registry.find_agents_by_capability(AgentCapability.VISION_LLM) == []
        
        # Status filtering
        registry.register("a1", "C", "m", [AgentCapability.OCR], priority=1)
        registry.register("a2", "C", "m", [AgentCapability.OCR], priority=2)
        registry.agents["a1"].status = "unhealthy"
        
        res = registry.find_agents_by_capability(AgentCapability.OCR, status="healthy")
        assert "a1" not in res
        assert "a2" in res
        
        # No status filter
        res_all = registry.find_agents_by_capability(AgentCapability.OCR, status=None)
        assert len(res_all) == 2

    def test_get_all_registrations(self, registry):
        registry.register("a1", "C", "m", [AgentCapability.OCR])
        d = registry.get_all_registrations()
        assert "a1" in d
        assert d["a1"]["capabilities"] == ["ocr"]

    def test_global_registry(self):
        # Reset global
        with patch("src.engine.agents.agent_registry._registry", None):
             r1 = get_agent_registry()
             r2 = get_agent_registry()
             assert r1 is r2
             assert "vision_agent" in r1.agents # default registration happened


# --- Multi-Agent Orchestrator Tests ---

class TestMultiAgentOrchestrator:
    
    @pytest.mark.asyncio
    async def test_simple_orchestration(self):
        # Mock LANGGRAPH_AVAILABLE = False
        with patch("src.engine.agents.multi_agent_orchestrator.LANGGRAPH_AVAILABLE", False):
            orch = MultiAgentOrchestrator()
            
            # Mock agents in registry
            mock_router = AsyncMock()
            mock_router.execute.return_value = AgentResult("router", True, {}, next_agent="analyzer")
            mock_router.name = "router"
            
            mock_analyzer = AsyncMock()
            mock_analyzer.execute.return_value = AgentResult("analyzer", True, {}, next_agent=None)
            mock_analyzer.name = "analyzer"
            
            orch.agents["router"] = mock_router
            orch.agents["analyzer"] = mock_analyzer
            
            # Run
            res = await orch.run("query")
            assert res["success"] is True
            assert len(res["results"]) == 2 # router + analyzer output

    @pytest.mark.asyncio
    async def test_simple_orchestration_error_handling(self):
        with patch("src.engine.agents.multi_agent_orchestrator.LANGGRAPH_AVAILABLE", False):
            orch = MultiAgentOrchestrator()
            # Router points to missing agent
            mock_router = AsyncMock()
            mock_router.execute.return_value = AgentResult("router", True, {}, next_agent="missing")
            mock_router.name = "router"
            orch.agents["router"] = mock_router
            
            res = await orch.run("query")
            assert "Agent not found: missing" in res["errors"]

    @pytest.mark.asyncio
    async def test_simple_orchestration_loop_logic(self):
         with patch("src.engine.agents.multi_agent_orchestrator.LANGGRAPH_AVAILABLE", False):
            orch = MultiAgentOrchestrator()
            
            # 1. Recommendations extraction logic
            mock_rec = AsyncMock()
            mock_rec.execute.return_value = AgentResult("rec", True, {"recommendations": ["r1"]}, next_agent=None)
            orch.agents["rec"] = mock_rec
            
            orch.register_agent(mock_rec) # Coverage for register_agent wrapper
            
            state = {
                "query": "q", "campaign_data": {}, "platform": None, 
                "analysis_results": [], "recommendations": [], 
                "current_agent": "rec", "iteration": 0, "max_iterations": 5, 
                "errors": []
            }
            res = await orch._simple_orchestrate(state)
            assert "r1" in res["recommendations"]
            
            # 2. Loop break on exception
            mock_err = AsyncMock()
            mock_err.execute.side_effect = Exception("Crash")
            orch.agents["err"] = mock_err
            
            state["current_agent"] = "err"
            state["iteration"] = 0
            res_err = await orch._simple_orchestrate(state)
            assert not res_err["success"]
            assert "Crash" in res_err["errors"][0]

    @pytest.mark.asyncio
    async def test_agent_classes(self):
        # Just verify execute logic for individual agents
        
        # Router
        router = RouterAgent()
        # Coverage for all branches
        for Keyword, Expected in [("spend", "budget_analyzer"), ("creative", "creative_analyzer"), ("audience", "audience_analyzer"), ("trend", "trend_analyzer")]:
             res = await router.execute({"query": f"analyze {Keyword}"})
             assert res.output["routed_to"] == Expected
        res = await router.execute({"query": "analyze spend and budget"})
        assert res.output["routed_to"] == "budget_analyzer"
        
        res = await router.execute({"query": "hello"})
        assert res.output["routed_to"] == "general_analyzer"
        
        # Analyzer
        analyzer = AnalyzerAgent()
        res = await analyzer.execute({"campaign_data": [{"spend": 100}]})
        assert res.output["analysis"]["total_spend"] == 100
        
        # Specialist
        spec = SpecialistAgent("meta")
        res = await spec.execute({})
        assert "Relevance Score" in res.output["specialist_insights"]["platform_specific_metrics"]
        
        # Recommender
        rec = RecommenderAgent()
        res = await rec.execute({})
        assert len(res.output["recommendations"]) > 0
        
        # Synthesizer
        syn = SynthesizerAgent()
        res = await syn.execute({"recommendations": ["Do X"]})
        assert res.output["final_synthesis"]["action_items"] == ["Do X"]

    @pytest.mark.asyncio
    async def test_langgraph_orchestration(self):
        # Mock LANGGRAPH_AVAILABLE = True
        with patch("src.engine.agents.multi_agent_orchestrator.LANGGRAPH_AVAILABLE", True):
            with patch("src.engine.agents.multi_agent_orchestrator.StateGraph") as MockGraph:
                # Setup mock graph behavior
                mock_compiled = AsyncMock()
                mock_compiled.ainvoke.return_value = {
                    "analysis_results": [{"k": "v"}],
                    "recommendations": ["rec"]
                }
                MockGraph.return_value.compile.return_value = mock_compiled
                
                orch = MultiAgentOrchestrator()
                assert orch._graph is not None
                
                res = await orch.run("query")
                assert res["success"] is True
                assert res["results"][0]["k"] == "v"
                
                # Verify node creation logic (harder to test without real langgraph, but we check if nodes added)
                MockGraph.return_value.add_node.assert_called()
                
                # Test _create_node inner function logic
                # We need to extract the node function created inside _build_graph or test it via helper
                # Since we can't easily access the closure, we can mock agents and simulate node execution if we could call it
                # But _create_node is protected. Let's call it directly.
                node_func = orch._create_node("test_agent")
                mock_agent = AsyncMock()
                mock_agent.execute.return_value = AgentResult("test", True, {"out": 1}, next_agent="next")
                orch.agents["test_agent"] = mock_agent
                
                state = {"analysis_results": []}
                new_state = await node_func(state)
                assert new_state["analysis_results"][0]["out"] == 1
                assert new_state["current_agent"] == "next"
                
                # Test agent not found in node
                node_func_missing = orch._create_node("missing")
                state_miss = {"analysis_results": []}
                # Should do nothing
                res_miss = await node_func_missing(state_miss)
                assert res_miss == state_miss

    def test_run_multi_agent_analysis_wrapper(self):
        with patch("src.engine.agents.multi_agent_orchestrator.MultiAgentOrchestrator.run", new_callable=AsyncMock) as mock_run:
            asyncio.run(run_multi_agent_analysis("q"))
            mock_run.assert_called()

    def test_global_orchestrator(self):
        with patch("src.engine.agents.multi_agent_orchestrator._orchestrator", None):
            with patch("src.engine.agents.multi_agent_orchestrator.MultiAgentOrchestrator") as mock_cls:
                # First call creates
                o1 = get_orchestrator()
                mock_cls.assert_called()
                
                # Second call reuses (simulate global set)
                # Since we patched _orchestrator to None initially, get_orchestrator sets it locally but our patch masks module attribute? 
                # No, get_orchestrator uses `global _orchestrator`.
                # We need to manually set it to verify the singleton logic if mock doesn't persist across patch scope properly for repeat calls?
                # Actually, standard pattern:
                pass 
                
        # Real singleton test without patch blocking the global var assignment
        # We can't easily reset the global without import access, but we can check if it returns same obj
        o1 = get_orchestrator()
        o2 = get_orchestrator()
        assert o1 is o2
