use crate::inverter::{Num::*, register::*, *};
use crate::modbus::Snapshot;
use homie5::HomieID;
use homie5::device_description::{
    DeviceDescriptionBuilder, HomieDeviceDescription, HomiePropertyDescription,
    NodeDescriptionBuilder, PropertyDescriptionBuilder,
};

pub(crate) const NAME: &str = "Sofar HYD inverter";

#[derive(Clone, Copy)]
enum Kind {
    Float(&'static str),
    Integer(&'static str),
    Percent,
    Count,
    Boolean,
    Text,
    Choice(&'static [&'static str]),
    Conditions,
    DateTime,
    Seconds,
    /// Settable only, for a momentary command.
    Trigger(&'static [&'static str]),
}
use Kind::*;

const VOLTS: Kind = Float(homie5::HOMIE_UNIT_VOLT);
const AMPERES: Kind = Float(homie5::HOMIE_UNIT_AMPERE);
const HERTZ: Kind = Float(homie5::HOMIE_UNIT_HERTZ);
const KILOWATT_HOURS: Kind = Float(homie5::HOMIE_UNIT_KILOWATTHOUR);
const WATTS: Kind = Integer(homie5::HOMIE_UNIT_WATT);
const VARS: Kind = Integer("var");
const CELSIUS: Kind = Integer(homie5::HOMIE_UNIT_DEGREE_CELSIUS);
const KILOHMS: Kind = Integer("kΩ");

impl Kind {
    fn describe(self, settable: bool) -> HomiePropertyDescription {
        use PropertyDescriptionBuilder as B;
        match self {
            Float(unit) => B::float().unit(unit).settable(settable).build(),
            Integer(unit) => B::integer().unit(unit).settable(settable).build(),
            Percent => B::integer().range(0..=100).unit(homie5::HOMIE_UNIT_PERCENT).build(),
            Count => B::integer().unit(homie5::HOMIE_UNIT_COUNT_AMOUNT).build(),
            Boolean => B::boolean().settable(settable).build(),
            Text => B::string().build(),
            Choice(values) => B::enumeration(values.iter().copied())
                .expect("enum values are valid")
                .settable(settable)
                .build(),
            Conditions => {
                B::json().json_schema(r#"{"type":"array","items":{"type":"string"}}"#).build()
            }
            DateTime => B::datetime().settable(settable).build(),
            Trigger(values) => B::enumeration(values.iter().copied())
                .expect("enum values are valid")
                .settable(true)
                .retained(false)
                .build(),
            Seconds => B::integer()
                .unit(homie5::HOMIE_UNIT_SECONDS)
                .range(0..=i64::from(u16::MAX))
                .settable(settable)
                .build(),
        }
    }
}

pub(crate) type Reader = Box<dyn Fn(&Snapshot) -> Reading>;
pub(crate) type Writer = Box<dyn Fn(&Snapshot, &str) -> Result<Write, Invalid>>;

type SetFn = fn(&Snapshot, &str) -> Result<Write, Invalid>;

struct Spec {
    id: HomieID,
    kind: Kind,
    read: fn(&Snapshot) -> Reading,
    write: Option<SetFn>,
}

impl Spec {
    const fn new(id: &'static str, kind: Kind, read: fn(&Snapshot) -> Reading) -> Self {
        Self { id: HomieID::new_const(id), kind, read, write: None }
    }

    const fn settable(mut self, write: SetFn) -> Self {
        self.write = Some(write);
        self
    }
}

static INVERTER: [Spec; 11] = [
    Spec::new("state", Choice(&STATES), state),
    Spec::new("faults", Conditions, faults),
    Spec::new("enabled", Boolean, enabled).settable(set_enabled),
    Spec::new("storage-mode", Choice(&STORAGE_MODES), storage_mode).settable(set_storage_mode),
    // Positive when the inverter puts power out, unlike the powers into the system.
    Spec::new("output-power", WATTS, |s| watts(s, OUTPUT_POWER, I16, true)),
    Spec::new("ambient-temperature", CELSIUS, |s| number(s, AMBIENT_TEMPERATURE, I16, 0)),
    Spec::new("heatsink-temperature", CELSIUS, |s| number(s, HEATSINK_TEMPERATURE, I16, 0)),
    Spec::new("internal-temperature", CELSIUS, |s| number(s, INTERNAL_TEMPERATURE, I16, 0)),
    Spec::new("insulation-resistance", KILOHMS, |s| number(s, INSULATION_RESISTANCE, U16, 0)),
    Spec::new("clock", DateTime, clock).settable(set_clock),
    Spec::new("sync-clock", Trigger(&SYNC_CLOCK), |_| None).settable(sync_clock),
];

static GRID: [Spec; 3] = [
    Spec::new("power", WATTS, |s| watts(s, GRID_POWER, I16, false)),
    Spec::new("reactive-power", VARS, |s| watts(s, GRID_REACTIVE_POWER, I16, true)),
    Spec::new("frequency", HERTZ, |s| number(s, GRID_FREQUENCY, U16, 2)),
];

static LOAD: [Spec; 1] = [Spec::new("power", WATTS, |s| watts(s, LOAD_POWER, U16, true))];

static EPS: [Spec; 3] = [
    Spec::new("mode", Choice(&EPS_MODES), eps_mode).settable(set_eps_mode),
    Spec::new("power", WATTS, |s| watts(s, EPS_POWER, I16, true)),
    Spec::new("frequency", HERTZ, |s| number(s, EPS_FREQUENCY, U16, 2)),
];

static ENERGY: [Spec; 12] = [
    Spec::new("solar-today", KILOWATT_HOURS, |s| number(s, PV_GENERATION_TODAY, U32, 2)),
    Spec::new("solar-total", KILOWATT_HOURS, |s| number(s, PV_GENERATION_TOTAL, U32, 1)),
    Spec::new("consumed-today", KILOWATT_HOURS, |s| number(s, CONSUMPTION_TODAY, U32, 2)),
    Spec::new("consumed-total", KILOWATT_HOURS, |s| number(s, CONSUMPTION_TOTAL, U32, 1)),
    Spec::new("imported-today", KILOWATT_HOURS, |s| number(s, IMPORT_TODAY, U32, 2)),
    Spec::new("imported-total", KILOWATT_HOURS, |s| number(s, IMPORT_TOTAL, U32, 1)),
    Spec::new("exported-today", KILOWATT_HOURS, |s| number(s, EXPORT_TODAY, U32, 2)),
    Spec::new("exported-total", KILOWATT_HOURS, |s| number(s, EXPORT_TOTAL, U32, 1)),
    Spec::new("charged-today", KILOWATT_HOURS, |s| number(s, CHARGE_TODAY, U32, 2)),
    Spec::new("charged-total", KILOWATT_HOURS, |s| number(s, CHARGE_TOTAL, U32, 1)),
    Spec::new("discharged-today", KILOWATT_HOURS, |s| number(s, DISCHARGE_TODAY, U32, 2)),
    Spec::new("discharged-total", KILOWATT_HOURS, |s| number(s, DISCHARGE_TOTAL, U32, 1)),
];

static PASSIVE: [Spec; 8] = [
    Spec::new("mode", Choice(&PASSIVE_MODES), passive_mode).settable(set_passive_mode),
    Spec::new("grid-power", WATTS, |s| passive_power(s, PASSIVE_GRID_POWER))
        .settable(|_, v| set_passive_power(PASSIVE_GRID_POWER, v)),
    Spec::new("battery-min-power", WATTS, |s| passive_power(s, PASSIVE_BATTERY_MIN_POWER))
        .settable(|_, v| set_passive_power(PASSIVE_BATTERY_MIN_POWER, v)),
    Spec::new("battery-max-power", WATTS, |s| passive_power(s, PASSIVE_BATTERY_MAX_POWER))
        .settable(|_, v| set_passive_power(PASSIVE_BATTERY_MAX_POWER, v)),
    Spec::new("export-limit", WATTS, |s| passive_power(s, PASSIVE_EXPORT_LIMIT))
        .settable(|_, v| set_passive_power(PASSIVE_EXPORT_LIMIT, v)),
    Spec::new("import-limit", WATTS, |s| passive_power(s, PASSIVE_IMPORT_LIMIT))
        .settable(|_, v| set_passive_power(PASSIVE_IMPORT_LIMIT, v)),
    Spec::new("timeout", Seconds, |s| number(s, PASSIVE_TIMEOUT, U16, 0))
        .settable(set_passive_timeout),
    Spec::new("timeout-action", Choice(&TIMEOUT_ACTIONS), passive_timeout_action)
        .settable(set_passive_timeout_action),
];

static IDENTIFICATION: [Spec; 6] = [
    Spec::new("serial-number", Text, serial_number),
    Spec::new("hardware-version", Text, hardware_version),
    Spec::new("communication-firmware", Text, |s| software_version(s, COMMUNICATION_VERSION)),
    Spec::new("primary-firmware", Text, |s| software_version(s, PRIMARY_VERSION)),
    Spec::new("secondary-firmware", Text, |s| software_version(s, SECONDARY_VERSION)),
    Spec::new("safety-version", Text, safety_version),
];

/// `write` is only there for settable properties.
pub(crate) struct Property {
    pub node: HomieID,
    pub id: HomieID,
    pub retained: bool,
    pub read: Reader,
    pub write: Option<Writer>,
}

pub(crate) struct Layout {
    pub description: HomieDeviceDescription,
    pub properties: Vec<Property>,
}

impl Layout {
    pub(crate) fn find(&self, node: &HomieID, id: &HomieID) -> Option<usize> {
        self.properties.iter().position(|p| &p.node == node && &p.id == id)
    }
}

struct Builder {
    writes: bool,
    nodes: Vec<(HomieID, homie5::device_description::HomieNodeDescription)>,
    properties: Vec<Property>,
}

impl Builder {
    fn node(
        &mut self,
        id: HomieID,
        name: String,
        properties: impl IntoIterator<Item = (HomieID, Kind, Reader, Option<Writer>)>,
    ) {
        let mut node = NodeDescriptionBuilder::new().name(name);
        for (property, kind, read, write) in properties {
            let write = write.filter(|_| self.writes);
            if matches!(kind, Trigger(_)) && write.is_none() {
                continue;
            }
            let description = kind.describe(write.is_some());
            let retained = description.retained;
            node = node.add_property(property.clone(), description);
            let property = Property { node: id.clone(), id: property, retained, read, write };
            self.properties.push(property);
        }
        self.nodes.push((id, node.build()));
    }

    fn fixed(&mut self, id: &'static str, name: &str, specs: &'static [Spec]) {
        let properties = specs.iter().map(|spec| {
            let read: Reader = Box::new(spec.read);
            let write = spec.write.map(|write| Box::new(write) as Writer);
            (spec.id.clone(), spec.kind, read, write)
        });
        self.node(HomieID::new_const(id), name.to_string(), properties);
    }
}

fn id(text: String) -> HomieID {
    HomieID::try_from(text).expect("a valid ID")
}

fn read_only(
    id: &'static str,
    kind: Kind,
    read: Reader,
) -> (HomieID, Kind, Reader, Option<Writer>) {
    (HomieID::new_const(id), kind, read, None)
}

fn phase(builder: &mut Builder, phase: u16) {
    let at = |first| of_phase(first, PHASE_STRIDE, phase);
    let eps = |first| of_phase(first, EPS_PHASE_STRIDE, phase);
    let (voltage, current, power) = (at(PHASE_VOLTAGE), at(PHASE_CURRENT), at(PHASE_POWER));
    let (eps_voltage, eps_current) = (eps(EPS_PHASE_VOLTAGE), eps(EPS_PHASE_CURRENT));
    let eps_power = eps(EPS_PHASE_POWER);
    let properties = [
        read_only("voltage", VOLTS, Box::new(move |s| number(s, voltage, U16, 1))),
        read_only("output-current", AMPERES, Box::new(move |s| number(s, current, U16, 2))),
        read_only("output-power", WATTS, Box::new(move |s| watts(s, power, I16, true))),
        read_only("eps-voltage", VOLTS, Box::new(move |s| number(s, eps_voltage, U16, 1))),
        read_only("eps-current", AMPERES, Box::new(move |s| number(s, eps_current, I16, 2))),
        read_only("eps-power", WATTS, Box::new(move |s| watts(s, eps_power, I16, true))),
    ];
    let number = phase + 1;
    builder.node(id(format!("l{number}")), format!("Phase L{number}"), properties);
}

fn pv_input(builder: &mut Builder, input: u16) {
    let (voltage, current, power) = (
        of_pv_input(PV_VOLTAGE, input),
        of_pv_input(PV_CURRENT, input),
        of_pv_input(PV_POWER, input),
    );
    let properties = [
        read_only("voltage", VOLTS, Box::new(move |s| number(s, voltage, U16, 1))),
        read_only("current", AMPERES, Box::new(move |s| number(s, current, U16, 2))),
        read_only("power", WATTS, Box::new(move |s| watts(s, power, U16, true))),
    ];
    let number = input + 1;
    builder.node(id(format!("pv-{number}")), format!("Solar input {number}"), properties);
}

fn battery(builder: &mut Builder, battery: u16, batteries: u16) {
    let at = |first| of_battery(first, battery);
    let (voltage, current, power) = (at(BATTERY_VOLTAGE), at(BATTERY_CURRENT), at(BATTERY_POWER));
    let (temperature, cycles) = (at(BATTERY_TEMPERATURE), at(BATTERY_CYCLES));
    let (charge, health) = (at(BATTERY_STATE_OF_CHARGE), at(BATTERY_STATE_OF_HEALTH));
    let properties = [
        read_only("voltage", VOLTS, Box::new(move |s| number(s, voltage, U16, 1))),
        read_only("current", AMPERES, Box::new(move |s| number(s, current, I16, 2))),
        read_only("power", WATTS, Box::new(move |s| watts(s, power, I16, true))),
        read_only("temperature", CELSIUS, Box::new(move |s| number(s, temperature, I16, 0))),
        read_only("state-of-charge", Percent, Box::new(move |s| percentage(s, charge))),
        read_only("state-of-health", Percent, Box::new(move |s| percentage(s, health))),
        read_only("cycles", Count, Box::new(move |s| number(s, cycles, U16, 0))),
    ];
    let (node, name) = match batteries {
        1 => ("battery".to_string(), "Battery".to_string()),
        _ => (format!("battery-{}", battery + 1), format!("Battery {}", battery + 1)),
    };
    builder.node(id(node), name, properties);
}

pub(crate) fn layout(equipment: Equipment, writes: bool, bridge: &HomieID) -> Layout {
    let mut builder = Builder { writes, nodes: Vec::new(), properties: Vec::new() };
    builder.fixed("inverter", "Inverter", &INVERTER);
    builder.fixed("grid", "Grid", &GRID);
    builder.fixed("load", "Load", &LOAD);
    builder.fixed("eps", "EPS output", &EPS);
    for p in 0..3 {
        phase(&mut builder, p);
    }
    let pv = [read_only("power", WATTS, Box::new(move |s| pv_power(s, equipment)))];
    let external =
        read_only("external-power", WATTS, Box::new(|s| watts(s, EXTERNAL_PV_POWER, U16, true)));
    builder.node(HomieID::new_const("pv"), "Solar".to_string(), pv.into_iter().chain([external]));
    for input in 0..equipment.pv_inputs {
        pv_input(&mut builder, input);
    }
    for b in 0..equipment.batteries {
        battery(&mut builder, b, equipment.batteries);
    }
    builder.fixed("energy", "Energy", &ENERGY);
    builder.fixed("passive", "Passive mode", &PASSIVE);
    builder.fixed("identification", "Identification", &IDENTIFICATION);
    let description = DeviceDescriptionBuilder::new().name(NAME).root(bridge.clone());
    let description =
        builder.nodes.into_iter().fold(description, |d, (id, node)| d.add_node(id, node));
    Layout { description: description.build(), properties: builder.properties }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inverter::tests::{EQUIPMENT, fixture};
    use std::collections::BTreeMap;

    fn values(layout: &Layout, snapshot: &Snapshot) -> BTreeMap<String, String> {
        let read = |p: &Property| (p.read)(snapshot).expect("has been read").expect("is valid");
        layout.properties.iter().map(|p| (format!("{}/{}", p.node, p.id), read(p))).collect()
    }

    #[test]
    fn the_fixture_is_decoded() {
        let bridge = HomieID::new_const("sofarhyd");
        let layout = layout(EQUIPMENT, false, &bridge);
        let values = values(&layout, &fixture());
        assert_eq!(values.len(), 10 + 3 + 1 + 3 + 3 * 6 + 2 + 2 * 3 + 7 + 12 + 8 + 6);
        for (path, value) in [
            ("inverter/state", "on-grid"),
            ("inverter/enabled", "true"),
            ("inverter/storage-mode", "passive"),
            ("inverter/output-power", "2500"),
            ("inverter/clock", "2026-10-27T22:47:05"),
            ("grid/power", "300"),
            ("load/power", "2800"),
            ("eps/mode", "enabled"),
            ("eps/frequency", "50.00"),
            ("l1/voltage", "230.1"),
            ("l2/output-power", "840"),
            ("l1/eps-voltage", "229.9"),
            ("pv/power", "4740"),
            ("pv/external-power", "0"),
            ("pv-2/voltage", "480.2"),
            ("battery/power", "1500"),
            ("battery/current", "3.75"),
            ("battery/state-of-charge", "67"),
            ("energy/imported-total", "703.1"),
            ("passive/battery-min-power", "-500"),
            ("passive/timeout", "600"),
            ("identification/primary-firmware", "V120005"),
        ] {
            assert_eq!(values.get(path).map(String::as_str), Some(value), "{path}");
        }
    }

    #[test]
    fn nothing_is_settable_without_writes() {
        let bridge = HomieID::new_const("sofarhyd");
        for writes in [false, true] {
            let layout = layout(EQUIPMENT, writes, &bridge);
            let settable: Vec<_> = layout
                .properties
                .iter()
                .filter(|p| p.write.is_some())
                .map(|p| format!("{}/{}", p.node, p.id))
                .collect();
            let expected = [
                "inverter/enabled",
                "inverter/storage-mode",
                "inverter/clock",
                "inverter/sync-clock",
                "eps/mode",
                "passive/mode",
                "passive/grid-power",
                "passive/battery-min-power",
                "passive/battery-max-power",
                "passive/export-limit",
                "passive/import-limit",
                "passive/timeout",
                "passive/timeout-action",
            ];
            assert_eq!(settable, if writes { &expected[..] } else { &[] });
            for property in &layout.properties {
                let node = &layout.description.nodes[&property.node];
                let described = &node.properties[&property.id];
                assert_eq!(described.settable, property.write.is_some(), "{}", property.id);
                assert_eq!(described.retained, property.id.as_str() != "sync-clock");
                assert_eq!(described.retained, property.retained);
            }
        }
    }

    #[test]
    fn the_equipment_makes_the_nodes() {
        let bridge = HomieID::new_const("sofarhyd");
        let layout = layout(Equipment { pv_inputs: 1, batteries: 2 }, false, &bridge);
        let mut nodes: Vec<_> = layout.description.nodes.keys().map(HomieID::as_str).collect();
        nodes.sort_unstable();
        let expected = [
            "battery-1",
            "battery-2",
            "energy",
            "eps",
            "grid",
            "identification",
            "inverter",
            "l1",
            "l2",
            "l3",
            "load",
            "passive",
            "pv",
            "pv-1",
        ];
        assert_eq!(nodes, expected);
        assert_eq!(layout.description.root.as_ref(), Some(&bridge));
        assert_eq!(
            layout.description.nodes[&HomieID::new_const("battery-2")].name.as_deref(),
            Some("Battery 2")
        );
        let battery = layout.find(&HomieID::new_const("battery-2"), &HomieID::new_const("cycles"));
        let mut snapshot = Snapshot::new();
        snapshot.set(0x060A + 7, 42);
        let read = &layout.properties[battery.unwrap()].read;
        assert_eq!(read(&snapshot), Some(Ok("42".to_string())));
    }
}
