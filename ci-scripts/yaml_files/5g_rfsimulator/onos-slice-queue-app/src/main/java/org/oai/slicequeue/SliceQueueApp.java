package org.oai.slicequeue;

import org.onlab.packet.EthType;
import org.onlab.packet.IPv4;
import org.onlab.packet.TpPort;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.net.Device;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Port;
import org.onosproject.net.PortNumber;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.flow.DefaultFlowRule;
import org.onosproject.net.flow.DefaultTrafficSelector;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.FlowRule;
import org.onosproject.net.flow.FlowRuleService;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.TrafficTreatment;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;

import java.util.Optional;

@Component(immediate = true)
public class SliceQueueApp {

    private static final String APP_NAME = "org.oai.slicequeue";
    private static final String UPF_PORT_NAME = "v-upf-host";
    private static final String EDN_PORT_NAME = "v-edn-host";

    @Reference
    protected CoreService coreService;

    @Reference
    protected DeviceService deviceService;

    @Reference
    protected FlowRuleService flowRuleService;

    private ApplicationId appId;

    @Activate
    protected void activate() {
        appId = coreService.registerApplication(APP_NAME);
        for (Device device : deviceService.getAvailableDevices()) {
            installSliceRules(device);
            break;
        }
    }

    @Deactivate
    protected void deactivate() {
        if (appId != null) {
            flowRuleService.removeFlowRulesById(appId);
        }
    }

    private void installSliceRules(Device device) {
        DeviceId did = device.id();
        Optional<PortNumber> upfPort = findPortByName(did, UPF_PORT_NAME);
        Optional<PortNumber> ednPort = findPortByName(did, EDN_PORT_NAME);
        if (upfPort.isEmpty() || ednPort.isEmpty()) {
            return;
        }

        addUdpQueueRule(did, upfPort.get(), ednPort.get(), 5201, 1, 40000); // eMBB
        addUdpQueueRule(did, upfPort.get(), ednPort.get(), 5202, 2, 50000); // URLLC
        addUdpQueueRule(did, upfPort.get(), ednPort.get(), 5203, 3, 30000); // mMTC
        addReverseRule(did, ednPort.get(), upfPort.get(), 20000);
    }

    private Optional<PortNumber> findPortByName(DeviceId did, String name) {
        return deviceService.getPorts(did).stream()
            .filter(p -> name.equals(p.annotations().value("portName"))
                || name.equals(p.annotations().value("name"))
                || name.equals(p.annotations().value("ifName")))
            .map(Port::number)
            .findFirst();
    }

    private void addUdpQueueRule(DeviceId did, PortNumber in, PortNumber out, int udpDst, long queue, int prio) {
        TrafficSelector selector = DefaultTrafficSelector.builder()
            .matchInPort(in)
            .matchEthType(EthType.EtherType.IPV4.ethType().toShort())
            .matchIPProtocol(IPv4.PROTOCOL_UDP)
            .matchUdpDst(TpPort.tpPort(udpDst))
            .build();

        TrafficTreatment treatment = DefaultTrafficTreatment.builder()
            .setQueue(queue)
            .setOutput(out)
            .build();

        applyRule(did, selector, treatment, prio);
    }

    private void addReverseRule(DeviceId did, PortNumber in, PortNumber out, int prio) {
        TrafficSelector selector = DefaultTrafficSelector.builder()
            .matchInPort(in)
            .matchEthType(EthType.EtherType.IPV4.ethType().toShort())
            .build();

        TrafficTreatment treatment = DefaultTrafficTreatment.builder()
            .setOutput(out)
            .build();

        applyRule(did, selector, treatment, prio);
    }

    private void applyRule(DeviceId did, TrafficSelector selector, TrafficTreatment treatment, int priority) {
        FlowRule rule = DefaultFlowRule.builder()
            .forDevice(did)
            .forTable(0)
            .fromApp(appId)
            .withPriority(priority)
            .withSelector(selector)
            .withTreatment(treatment)
            .makePermanent()
            .build();
        flowRuleService.applyFlowRules(rule);
    }
}
