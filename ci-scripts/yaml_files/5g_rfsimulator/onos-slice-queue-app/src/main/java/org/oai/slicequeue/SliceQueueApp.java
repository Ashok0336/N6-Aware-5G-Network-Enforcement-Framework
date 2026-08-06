package org.oai.slicequeue;

import org.onlab.packet.EthType;
import org.onlab.packet.Ethernet;
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
    private static final long DEFAULT_UPF_PORT = 1;
    private static final long DEFAULT_EDN_PORT = 2;

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
        info("Registered appId name=" + APP_NAME + " id=" + appId.id());
        Optional<Device> selected = selectDevice();
        if (selected.isEmpty()) {
            warn("No available device found for " + APP_NAME);
            return;
        }
        installSliceRules(selected.get());
        info("Activated " + APP_NAME + " on device " + selected.get().id());
    }

    private Optional<Device> selectDevice() {
        String configuredDevice = configured("DEVICE_ID", "org.oai.slicequeue.deviceId");
        if (configuredDevice != null && !configuredDevice.isBlank()) {
            DeviceId configuredId = DeviceId.deviceId(configuredDevice.trim());
            Device device = deviceService.getDevice(configuredId);
            if (device != null && deviceService.isAvailable(configuredId)) {
                return Optional.of(device);
            }
            warn("Configured device " + configuredDevice + " is not available; falling back to the first available device");
        }
        for (Device device : deviceService.getAvailableDevices()) {
            return Optional.of(device);
        }
        return Optional.empty();
    }

    private String configured(String envName, String propertyName) {
        String value = System.getenv(envName);
        if (value != null && !value.isBlank()) {
            return value;
        }
        return System.getProperty(propertyName);
    }

    @Deactivate
    protected void deactivate() {
        if (appId != null) {
            flowRuleService.removeFlowRulesById(appId);
        }
    }

    private void installSliceRules(Device device) {
        DeviceId did = device.id();
        PortNumber upfPort = resolvePort(did, "UPF_PORT", "org.oai.slicequeue.upfPort", UPF_PORT_NAME, DEFAULT_UPF_PORT);
        PortNumber ednPort = resolvePort(did, "EDN_PORT", "org.oai.slicequeue.ednPort", EDN_PORT_NAME, DEFAULT_EDN_PORT);

        info("Installing slice queue rules device=" + did + " upfPort=" + upfPort + " ednPort=" + ednPort);
        addUdpDstQueueRule(did, "forward", upfPort, ednPort, 5201, 1, 40000);
        addUdpDstQueueRule(did, "forward", upfPort, ednPort, 5202, 2, 50000);
        addUdpDstQueueRule(did, "forward", upfPort, ednPort, 5203, 3, 45000);
        addUdpSrcQueueRule(did, "reverse_observed", ednPort, upfPort, 5201, 1, 40000);
        addUdpSrcQueueRule(did, "reverse_observed", ednPort, upfPort, 5202, 2, 50000);
        addUdpSrcQueueRule(did, "reverse_observed", ednPort, upfPort, 5203, 3, 45000);
        addUdpDstQueueRule(did, "reverse_diagnostic", ednPort, upfPort, 5201, 1, 39000);
        addUdpDstQueueRule(did, "reverse_diagnostic", ednPort, upfPort, 5202, 2, 49000);
        addUdpDstQueueRule(did, "reverse_diagnostic", ednPort, upfPort, 5203, 3, 44000);
        addArpRule(did, upfPort, ednPort, 45000);
        addArpRule(did, ednPort, upfPort, 45000);
        addIpv4Rule(did, upfPort, ednPort, 5000);
        addIpv4Rule(did, ednPort, upfPort, 5000);
    }

    private PortNumber resolvePort(DeviceId did, String envName, String propertyName, String annotationName, long defaultPort) {
        String configuredPort = configured(envName, propertyName);
        if (configuredPort != null && !configuredPort.isBlank()) {
            try {
                return PortNumber.portNumber(Long.parseLong(configuredPort.trim()));
            } catch (NumberFormatException e) {
                warn("Ignoring invalid port value " + envName + "=" + configuredPort);
            }
        }

        Optional<PortNumber> annotated = findPortByName(did, annotationName);
        if (annotated.isPresent()) {
            return annotated.get();
        }

        warn("Could not discover " + annotationName + "; using default port " + defaultPort);
        return PortNumber.portNumber(defaultPort);
    }

    private Optional<PortNumber> findPortByName(DeviceId did, String name) {
        return deviceService.getPorts(did).stream()
            .filter(p -> name.equals(p.annotations().value("portName"))
                || name.equals(p.annotations().value("name"))
                || name.equals(p.annotations().value("ifName")))
            .map(Port::number)
            .findFirst();
    }

    private void addUdpDstQueueRule(DeviceId did, String direction, PortNumber in, PortNumber out, int udpDst, long queueId, int prio) {
        TrafficSelector selector = DefaultTrafficSelector.builder()
            .matchInPort(in)
            .matchEthType(Ethernet.TYPE_IPV4)
            .matchIPProtocol(IPv4.PROTOCOL_UDP)
            .matchUdpDst(TpPort.tpPort(udpDst))
            .build();

        TrafficTreatment treatment = DefaultTrafficTreatment.builder()
            .setQueue(queueId)
            .setOutput(out)
            .build();

        info("queue-rule direction=" + direction
            + " inPort=" + in
            + " udpDst=" + udpDst
            + " queueId=" + queueId
            + " outputPort=" + out);
        applyRule(did, selector, treatment, prio);
    }

    private void addUdpSrcQueueRule(DeviceId did, String direction, PortNumber in, PortNumber out, int udpSrc, long queueId, int prio) {
        TrafficSelector selector = DefaultTrafficSelector.builder()
            .matchInPort(in)
            .matchEthType(Ethernet.TYPE_IPV4)
            .matchIPProtocol(IPv4.PROTOCOL_UDP)
            .matchUdpSrc(TpPort.tpPort(udpSrc))
            .build();

        TrafficTreatment treatment = DefaultTrafficTreatment.builder()
            .setQueue(queueId)
            .setOutput(out)
            .build();

        info("queue-rule direction=" + direction
            + " inPort=" + in
            + " udpSrc=" + udpSrc
            + " queueId=" + queueId
            + " outputPort=" + out);
        applyRule(did, selector, treatment, prio);
    }

    private void addIpv4Rule(DeviceId did, PortNumber in, PortNumber out, int prio) {
        TrafficSelector selector = DefaultTrafficSelector.builder()
            .matchInPort(in)
            .matchEthType(EthType.EtherType.IPV4.ethType().toShort())
            .build();

        TrafficTreatment treatment = DefaultTrafficTreatment.builder()
            .setOutput(out)
            .build();

        applyRule(did, selector, treatment, prio);
    }

    private void addArpRule(DeviceId did, PortNumber in, PortNumber out, int prio) {
        TrafficSelector selector = DefaultTrafficSelector.builder()
            .matchInPort(in)
            .matchEthType(EthType.EtherType.ARP.ethType().toShort())
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

    private void info(String message) {
        System.out.println("[org.oai.slicequeue] " + message);
    }

    private void warn(String message) {
        System.err.println("[org.oai.slicequeue] WARN " + message);
    }
}
