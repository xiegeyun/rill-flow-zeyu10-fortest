package com.weibo.rill.flow.olympicene.traversal

import com.weibo.rill.flow.interfaces.model.task.TaskStatus
import com.weibo.rill.flow.olympicene.core.event.Callback
import com.weibo.rill.flow.olympicene.core.event.Event
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus
import com.weibo.rill.flow.olympicene.core.runtime.DAGParser
import com.weibo.rill.flow.olympicene.core.runtime.DAGStorageProcedure
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager
import com.weibo.rill.flow.olympicene.ddl.parser.DAGStringParser
import com.weibo.rill.flow.olympicene.ddl.serialize.YAMLSerializer
import com.weibo.rill.flow.olympicene.ddl.validation.dag.impl.FlowDAGValidator
import com.weibo.rill.flow.olympicene.storage.save.impl.DAGLocalStorage
import com.weibo.rill.flow.olympicene.storage.save.impl.LocalStorageProcedure
import com.weibo.rill.flow.olympicene.traversal.callback.DAGCallbackInfo
import com.weibo.rill.flow.olympicene.traversal.callback.DAGEvent
import com.weibo.rill.flow.olympicene.traversal.checker.DefaultTimeChecker
import com.weibo.rill.flow.olympicene.traversal.config.OlympiceneFacade
import com.weibo.rill.flow.olympicene.traversal.dispatcher.DAGDispatcher
import io.opentelemetry.api.trace.Tracer
import spock.lang.Specification

class SwitchTaskTraversalTest extends Specification {
    DAGParser dagParser = new DAGStringParser(new YAMLSerializer(), [new FlowDAGValidator()])
    DAGLocalStorage dagStorage = new DAGLocalStorage()
    Callback callback = Mock(Callback.class)
    DAGDispatcher dispatcher = Mock(DAGDispatcher.class)
    DAGStorageProcedure dagStorageProcedure = new LocalStorageProcedure()
    SwitcherManager switcherManager = Mock(SwitcherManager.class)
    Olympicene olympicene = OlympiceneFacade.build(dagStorage, dagStorage, callback, dispatcher, dagStorageProcedure, Mock(DefaultTimeChecker.class), switcherManager, Mock(Tracer))

    def "test basic switch"() {
        given:
        String text = "workspace: default\n" +
                "dagName: testSwitch2\n" +
                "alias: release\n" +
                "type: flow\n" +
                "inputSchema: '[{\"required\":true,\"name\":\"input\",\"type\":\"Number\",\"desc\":\"\"}]'\n" +
                "tasks:\n" +
                "  - next: caseD\n" +
                "    name: caseA\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - next: caseE\n" +
                "    name: caseB\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - next: caseF\n" +
                "    name: caseC\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - name: caseD\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - name: caseE\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - name: caseF\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - name: switch\n" +
                "    switches:\n" +
                "      - next: caseA\n" +
                "        condition: \$.input.[?(@.input == 0)]\n" +
                "      - next: caseB\n" +
                "        condition: \$.input.[?(@.input == 5)]\n" +
                "      - next: caseC\n" +
                "        condition: \$.input.[?(@.input == 10)]\n" +
                "    description: ''\n" +
                "    inputMappings:\n" +
                "      - source: \$.context.input\n" +
                "        target: \$.input.input\n" +
                "    category: switch\n" +
                "    title: ''\n"
        DAG dag = dagParser.parse(text)

        when:
        olympicene.submit("executionIdSuccess", dag, ['input':0])

        then:
        noExceptionThrown()
        1 * callback.onEvent({
            Event event -> {
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                        ((DAGCallbackInfo)event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseA').taskStatus == TaskStatus.SUCCEED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseB').taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseC').taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseD').taskStatus == TaskStatus.SUCCEED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseE').taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseF').taskStatus == TaskStatus.SKIPPED
            }
        })
    }

    def "test empty switch"() {
        given:
        String text = "workspace: default\n" +
                "dagName: testSwitch\n" +
                "alias: release\n" +
                "type: flow\n" +
                "inputSchema: '[{\"required\":true,\"name\":\"input\",\"type\":\"Number\",\"desc\":\"\"}]'\n" +
                "tasks:\n" +
                "  - name: switch\n" +
                "    switches: []\n" +
                "    description: ''\n" +
                "    inputMappings:\n" +
                "      - source: \$.context.input\n" +
                "        target: \$.input.input\n" +
                "    category: switch\n" +
                "    title: ''\n"
        DAG dag = dagParser.parse(text)

        when:
        olympicene.submit("executionIdSuccess", dag, ['input':0])

        then:
        noExceptionThrown()
        1 * callback.onEvent({
            Event event -> {
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                        ((DAGCallbackInfo)event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('switch').taskStatus == TaskStatus.SUCCEED
            }
        })
    }

    def "test error condition"() {
        given:
        String text = "workspace: default\n" +
                "dagName: testSwitch\n" +
                "alias: release\n" +
                "type: flow\n" +
                "inputSchema: '[{\"required\":true,\"name\":\"input\",\"type\":\"Number\",\"desc\":\"\"}]'\n" +
                "tasks:\n" +
                "  - name: caseA\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - name: switch\n" +
                "    switches:\n" +
                "      - next: caseA\n" +
                "        condition: \$\$\$\n" +
                "    description: ''\n" +
                "    inputMappings:\n" +
                "      - source: \$.context.input\n" +
                "        target: \$.input.input\n" +
                "    category: switch\n" +
                "    title: ''\n"
        DAG dag = dagParser.parse(text)

        when:
        olympicene.submit("executionIdSuccess", dag, ['input':0])

        then:
        noExceptionThrown()
        1 * callback.onEvent({
            Event event -> {
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                        ((DAGCallbackInfo)event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('switch').taskStatus == TaskStatus.SUCCEED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseA').taskStatus == TaskStatus.SKIPPED
            }
        })
    }

    def "test switch condition crosses"() {
        given:
        String text = "workspace: default\n" +
                "dagName: testSwitch\n" +
                "alias: release\n" +
                "type: flow\n" +
                "inputSchema: '[{\"required\":true,\"name\":\"input\",\"type\":\"Number\",\"desc\":\"\"}]'\n" +
                "tasks:\n" +
                "  - next: ''\n" +
                "    name: caseA\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - next: ''\n" +
                "    name: caseB\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - name: caseC\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - name: switch\n" +
                "    switches:\n" +
                "      - next: caseA,caseC\n" +
                "        condition: \$.input.[?(@.input == 0)]\n" +
                "      - next: caseA,caseB\n" +
                "        condition: \$.input.[?(@.input == 5)]\n" +
                "      - next: caseB\n" +
                "        condition: \$.input.[?(@.input == 10)]\n" +
                "    description: ''\n" +
                "    inputMappings:\n" +
                "      - source: \$.context.input\n" +
                "        target: \$.input.input\n" +
                "    category: switch\n" +
                "    title: ''\n"
        DAG dag = dagParser.parse(text)

        when:
        olympicene.submit("executionIdSuccess", dag, ['input':0])

        then:
        noExceptionThrown()
        1 * callback.onEvent({
            Event event -> {
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                        ((DAGCallbackInfo)event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseA').taskStatus == TaskStatus.SUCCEED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseB').taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseC').taskStatus == TaskStatus.SUCCEED
            }
        })
    }

    def "test 2 switch tasks cross"() {
        given:
        String text = "workspace: default\n" +
                "dagName: testSwitch4\n" +
                "alias: release\n" +
                "type: flow\n" +
                "inputSchema: >-\n" +
                "  [{\"required\":true,\"name\":\"input\",\"type\":\"Number\",\"desc\":\"input\"},{\"required\":true,\"name\":\"trigger\",\"type\":\"Boolean\",\"desc\":\"trigger\"}]\n" +
                "tasks:\n" +
                "  - next: caseD\n" +
                "    name: caseA\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - next: caseD,caseF\n" +
                "    name: caseB\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - next: caseF,caseE\n" +
                "    name: caseC\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - name: caseD\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - name: caseE\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - name: switch1\n" +
                "    switches:\n" +
                "      - next: caseB\n" +
                "        condition: \$.input.[?(@.input == 0)]\n" +
                "      - next: caseC\n" +
                "        condition: \$.input.[?(@.input == 2)]\n" +
                "      - next: caseA\n" +
                "        condition: \$.input.[?(@.input == 10)]\n" +
                "    description: ''\n" +
                "    inputMappings:\n" +
                "      - source: \$.context.input\n" +
                "        target: \$.input.input\n" +
                "    category: switch\n" +
                "    title: ''\n" +
                "  - name: caseF\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - name: switch\n" +
                "    switches:\n" +
                "      - next: caseA\n" +
                "        condition: \$.input.[?(@.input == 0)]\n" +
                "      - next: caseB\n" +
                "        condition: \$.input.[?(@.input == 5)]\n" +
                "      - next: caseC\n" +
                "        condition: \$.input.[?(@.input == 10)]\n" +
                "    description: ''\n" +
                "    inputMappings:\n" +
                "      - source: \$.context.input\n" +
                "        target: \$.input.input\n" +
                "    category: switch\n" +
                "    title: ''\n"
        DAG dag = dagParser.parse(text)

        when:
        olympicene.submit("executionIdSuccess", dag, ['input':0])

        then:
        noExceptionThrown()
        1 * callback.onEvent({
            Event event -> {
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                        ((DAGCallbackInfo)event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseA').taskStatus == TaskStatus.SUCCEED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseB').taskStatus == TaskStatus.SUCCEED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseC').taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseD').taskStatus == TaskStatus.SUCCEED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseE').taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseF').taskStatus == TaskStatus.SUCCEED
            }
        })
    }

    def "test switch with break"() {
        given:
        String text = "workspace: default\n" +
                "dagName: testSwitch5\n" +
                "alias: release\n" +
                "type: flow\n" +
                "inputSchema: '[{\"required\":true,\"name\":\"input\",\"type\":\"Number\",\"desc\":\"input\"}]'\n" +
                "tasks:\n" +
                "  - name: caseA\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - name: caseB\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - name: caseC\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - name: caseD\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - name: switch\n" +
                "    switches:\n" +
                "      - next: caseA\n" +
                "        condition: \$.input.[?(@.input == 0)]\n" +
                "        break: true\n" +
                "      - next: caseB\n" +
                "        condition: \$.input.[?(@.input <= 5)]\n" +
                "        break: true\n" +
                "      - next: caseC\n" +
                "        condition: \$.input.[?(@.input <= 10)]\n" +
                "        break: true\n" +
                "      - next: caseD\n" +
                "        condition: default\n" +
                "        break: true\n" +
                "    description: ''\n" +
                "    inputMappings:\n" +
                "      - source: \$.context.input\n" +
                "        target: \$.input.input\n" +
                "    category: switch\n" +
                "    title: ''\n"
        DAG dag = dagParser.parse(text)

        when:
        olympicene.submit("executionIdSuccess", dag, ['input':0])

        then:
        noExceptionThrown()
        1 * callback.onEvent({
            Event event -> {
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                        ((DAGCallbackInfo)event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseA').taskStatus == TaskStatus.SUCCEED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseB').taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseC').taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseD').taskStatus == TaskStatus.SKIPPED
            }
        })
    }

    def "test switch with default"() {
        given:
        String text = "workspace: default\n" +
                "dagName: testSwitch5\n" +
                "alias: release\n" +
                "type: flow\n" +
                "inputSchema: '[{\"required\":true,\"name\":\"input\",\"type\":\"Number\",\"desc\":\"input\"}]'\n" +
                "tasks:\n" +
                "  - name: caseA\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - name: caseB\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - name: caseC\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - name: caseD\n" +
                "    description: ''\n" +
                "    category: pass\n" +
                "    title: ''\n" +
                "  - name: switch\n" +
                "    switches:\n" +
                "      - next: caseA\n" +
                "        condition: \$.input.[?(@.input == 0)]\n" +
                "        break: true\n" +
                "      - next: caseB\n" +
                "        condition: \$.input.[?(@.input <= 5)]\n" +
                "        break: true\n" +
                "      - next: caseC\n" +
                "        condition: \$.input.[?(@.input <= 10)]\n" +
                "        break: true\n" +
                "      - next: caseD\n" +
                "        condition: default\n" +
                "        break: true\n" +
                "    description: ''\n" +
                "    inputMappings:\n" +
                "      - source: \$.context.input\n" +
                "        target: \$.input.input\n" +
                "    category: switch\n" +
                "    title: ''\n"
        DAG dag = dagParser.parse(text)

        when:
        olympicene.submit("executionIdSuccess", dag, ['input':15])

        then:
        noExceptionThrown()
        1 * callback.onEvent({
            Event event -> {
                event.eventCode == DAGEvent.DAG_SUCCEED.getCode() &&
                        ((DAGCallbackInfo)event.data).dagInfo.dagStatus == DAGStatus.SUCCEED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseA').taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseB').taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseC').taskStatus == TaskStatus.SKIPPED &&
                        ((DAGCallbackInfo)event.data).dagInfo.tasks.get('caseD').taskStatus == TaskStatus.SUCCEED
            }
        })
    }

    def "test switch in sub task"() {
        given:
        String text = "workspace: default\n" +
                "dagName: switchSubTask\n" +
                "alias: release\n" +
                "type: flow\n" +
                "inputSchema: '[]'\n" +
                "tasks:\n" +
                "  - name: foreach\n" +
                "    description: ''\n" +
                "    synchronization:\n" +
                "      conditions: []\n" +
                "    iterationMapping:\n" +
                "      item: info\n" +
                "      index: index\n" +
                "      collection: \$.input.infos_array\n" +
                "    inputMappings:\n" +
                "      - transform: return seq.list(0, 1);\n" +
                "        target: \$.input.infos_array\n" +
                "    category: foreach\n" +
                "    tasks:\n" +
                "      - name: A\n" +
                "        description: ''\n" +
                "        category: pass\n" +
                "        title: ''\n" +
                "      - name: B\n" +
                "        description: ''\n" +
                "        category: pass\n" +
                "        title: ''\n" +
                "      - next: A,B\n" +
                "        name: switch\n" +
                "        switches:\n" +
                "          - next: A\n" +
                "            condition: \$.input.[?(@.index == 0)]\n" +
                "          - next: B\n" +
                "            condition: \$.input.[?(@.index == 1)]\n" +
                "        description: ''\n" +
                "        inputMappings:\n" +
                "          - source: \$.context.index\n" +
                "            target: \$.input.index\n" +
                "        category: switch\n" +
                "        title: ''\n"
        DAG dag = dagParser.parse(text)

        when:
        olympicene.submit("executionIdSuccess", dag, ['input':15])

        then:
        noExceptionThrown()
        1 * callback.onEvent({
            Event event -> {
                event.eventCode == DAGEvent.TASK_FINISH.getCode() && event.getData() instanceof DAGCallbackInfo
                        && ((DAGCallbackInfo) event.getData()).getTaskInfo().getName().equals("foreach_0-A")
                        && ((DAGCallbackInfo) event.getData()).getTaskInfo().getTaskStatus() == TaskStatus.SUCCEED
            }
        })
        1 * callback.onEvent({
            Event event -> {
                event.eventCode == DAGEvent.TASK_SKIPPED.getCode() && event.getData() instanceof DAGCallbackInfo
                        && ((DAGCallbackInfo) event.getData()).getTaskInfo().getName().equals("foreach_0-B")
                        && ((DAGCallbackInfo) event.getData()).getTaskInfo().getTaskStatus() == TaskStatus.SKIPPED
            }
        })
        1 * callback.onEvent({
            Event event -> {
                event.eventCode == DAGEvent.TASK_SKIPPED.getCode() && event.getData() instanceof DAGCallbackInfo
                        && ((DAGCallbackInfo) event.getData()).getTaskInfo().getName().equals("foreach_1-A")
                        && ((DAGCallbackInfo) event.getData()).getTaskInfo().getTaskStatus() == TaskStatus.SKIPPED
            }
        })
        1 * callback.onEvent({
            Event event -> {
                event.eventCode == DAGEvent.TASK_FINISH.getCode() && event.getData() instanceof DAGCallbackInfo
                        && ((DAGCallbackInfo) event.getData()).getTaskInfo().getName().equals("foreach_1-B")
                        && ((DAGCallbackInfo) event.getData()).getTaskInfo().getTaskStatus() == TaskStatus.SUCCEED
            }
        })
    }
}
