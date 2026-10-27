//! What the registers mean. A reading is `None` if a register it needs has
//! not been read yet and `Some(Err(_))` if the inverter said something unusable.
//!
//! Powers are positive going into the system (buying, charging) to match the passive mode settings.

use crate::modbus::{PollBlock, Snapshot};
use std::collections::BTreeSet;
use std::time::Duration;

const CONSTANT_PERIOD: Duration = Duration::from_secs(10 * 60);
const ENERGY_PERIOD: Duration = Duration::from_secs(15);
const SETTINGS_PERIOD: Duration = Duration::from_secs(30);

pub(crate) const MAX_PV_INPUTS: u16 = 4;
pub(crate) const MAX_BATTERIES: u16 = 4;

const PV_INPUT_REGISTERS: u16 = 3;
const BATTERY_REGISTERS: u16 = 7;

#[derive(Clone, Copy)]
struct Block {
    start: u16,
    count: u16,
}

impl Block {
    const fn through(start: u16, last: u16) -> Block {
        Block { start, count: last - start + 1 }
    }
}

const STATUS: Block = Block::through(0x0404, 0x0431);
const IDENTIFICATION: Block = Block::through(0x0445, 0x045C);
const INPUT_TYPES: Block = Block::through(0x1011, 0x1020);
const PV: Block = Block { start: reg::PV_VOLTAGE, count: MAX_PV_INPUTS * PV_INPUT_REGISTERS };
const BATTERIES: Block =
    Block { start: reg::BATTERY_VOLTAGE, count: MAX_BATTERIES * BATTERY_REGISTERS };
const GRID: Block = Block::through(0x0484, 0x04AF);
const EPS_OUTPUT: Block = Block::through(0x0504, 0x051F);
const ENERGY: Block = Block::through(0x0684, 0x069B);
const EPS_SETTINGS: Block = Block::through(0x1029, 0x102A);
const CONTROL: Block = Block::through(0x1104, 0x1110);
const PASSIVE: Block = Block::through(0x1184, 0x11A9);

pub(crate) mod register {
    pub const STATE: u16 = 0x0404;
    pub const FAULTS: u16 = 0x0405;
    pub const AMBIENT_TEMPERATURE: u16 = 0x0418;
    pub const HEATSINK_TEMPERATURE: u16 = 0x041A;
    pub const INTERNAL_TEMPERATURE: u16 = 0x0420;
    pub const INSULATION_RESISTANCE: u16 = 0x042B;
    pub const CLOCK: u16 = 0x042C;
    pub const SERIAL_NUMBER: u16 = 0x0445;
    pub const HARDWARE_VERSION: u16 = 0x044D;
    pub const COMMUNICATION_VERSION: u16 = 0x044F;
    pub const PRIMARY_VERSION: u16 = 0x0453;
    pub const SECONDARY_VERSION: u16 = 0x0457;
    pub const SAFETY_VERSION: u16 = 0x045B;
    pub const GRID_FREQUENCY: u16 = 0x0484;
    pub const OUTPUT_POWER: u16 = 0x0485;
    pub const GRID_POWER: u16 = 0x0488;
    pub const GRID_REACTIVE_POWER: u16 = 0x0489;
    pub const PHASE_STRIDE: u16 = 0x0B;
    pub const PHASE_VOLTAGE: u16 = 0x048D;
    pub const PHASE_CURRENT: u16 = 0x048E;
    pub const PHASE_POWER: u16 = 0x048F;
    pub const EXTERNAL_PV_POWER: u16 = 0x04AE;
    pub const LOAD_POWER: u16 = 0x04AF;
    pub const EPS_POWER: u16 = 0x0504;
    pub const EPS_FREQUENCY: u16 = 0x0507;
    pub const EPS_PHASE_STRIDE: u16 = 0x08;
    pub const EPS_PHASE_VOLTAGE: u16 = 0x050A;
    pub const EPS_PHASE_CURRENT: u16 = 0x050B;
    pub const EPS_PHASE_POWER: u16 = 0x050C;
    pub const PV_VOLTAGE: u16 = 0x0584;
    pub const PV_CURRENT: u16 = 0x0585;
    pub const PV_POWER: u16 = 0x0586;
    pub const BATTERY_VOLTAGE: u16 = 0x0604;
    pub const BATTERY_CURRENT: u16 = 0x0605;
    pub const BATTERY_POWER: u16 = 0x0606;
    pub const BATTERY_TEMPERATURE: u16 = 0x0607;
    pub const BATTERY_STATE_OF_CHARGE: u16 = 0x0608;
    pub const BATTERY_STATE_OF_HEALTH: u16 = 0x0609;
    pub const BATTERY_CYCLES: u16 = 0x060A;
    pub const PV_GENERATION_TODAY: u16 = 0x0684;
    pub const PV_GENERATION_TOTAL: u16 = 0x0686;
    pub const CONSUMPTION_TODAY: u16 = 0x0688;
    pub const CONSUMPTION_TOTAL: u16 = 0x068A;
    pub const IMPORT_TODAY: u16 = 0x068C;
    pub const IMPORT_TOTAL: u16 = 0x068E;
    pub const EXPORT_TODAY: u16 = 0x0690;
    pub const EXPORT_TOTAL: u16 = 0x0692;
    pub const CHARGE_TODAY: u16 = 0x0694;
    pub const CHARGE_TOTAL: u16 = 0x0696;
    pub const DISCHARGE_TODAY: u16 = 0x0698;
    pub const DISCHARGE_TOTAL: u16 = 0x069A;
    pub const CLOCK_SETTING: u16 = 0x1004;
    pub const CLOCK_CONTROL: u16 = 0x100A;
    pub const EPS_CONTROL: u16 = 0x1029;
    pub const EPS_WAIT_TIME: u16 = 0x102A;
    pub const REMOTE_SWITCH: u16 = 0x1104;
    pub const STORAGE_MODE: u16 = 0x1110;
    pub const PASSIVE_TIMEOUT: u16 = 0x1184;
    pub const PASSIVE_TIMEOUT_ACTION: u16 = 0x1185;
    pub const PASSIVE_GRID_POWER: u16 = 0x1187;
    pub const PASSIVE_BATTERY_MIN_POWER: u16 = 0x1189;
    pub const PASSIVE_BATTERY_MAX_POWER: u16 = 0x118B;
    pub const PASSIVE_EXPORT_LIMIT: u16 = 0x118D;
    pub const PASSIVE_IMPORT_LIMIT: u16 = 0x118F;
    pub const PASSIVE_MODE: u16 = 0x11A9;
}
use register as reg;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Invalid(pub &'static str);

impl std::fmt::Display for Invalid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

pub(crate) type Reading = Option<Result<String, Invalid>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Write {
    pub start: u16,
    pub words: Vec<u16>,
}

/// Writing 1 to one of these makes the inverter take the registers before it. It reads back 0 once
/// that is done and 1 while it is under way; anything else is a failure.
const COMMITS: [u16; 1] = [reg::CLOCK_CONTROL];

impl Write {
    /// Whether the registers read back as the inverter having taken what was written.
    pub(crate) fn taken(&self, read_back: &[u16]) -> Result<(), Invalid> {
        if read_back.len() != self.words.len() {
            return Err(Invalid("not all of the registers were read back"));
        }
        let words = self.words.iter().zip(read_back);
        for (address, (&wrote, &read)) in (self.start..).zip(words) {
            match COMMITS.contains(&address) {
                true if read > 1 => return Err(Invalid("the inverter has refused the values")),
                false if read != wrote => return Err(Invalid("the inverter has not taken it")),
                _ => {}
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Equipment {
    pub pv_inputs: u16,
    pub batteries: u16,
}

/// The inputs that are not in use read 0, so they are polled regardless: the equipment is only
/// known once the input types have been read.
pub(crate) fn poll_plan(period: Duration) -> Vec<PollBlock> {
    let changing = [STATUS, GRID, EPS_OUTPUT, PV, BATTERIES].map(|block| (block, period));
    let energy = (ENERGY, ENERGY_PERIOD.max(period));
    let settings =
        [EPS_SETTINGS, CONTROL, PASSIVE].map(|block| (block, SETTINGS_PERIOD.max(period)));
    let constant = [IDENTIFICATION, INPUT_TYPES].map(|block| (block, CONSTANT_PERIOD));
    let blocks = constant.into_iter().chain(changing);
    let blocks = blocks.chain([energy]).chain(settings);
    let poll = |(Block { start, count }, period)| PollBlock { start, count, period };
    blocks.map(poll).collect()
}

fn word(snapshot: &Snapshot, address: u16) -> Option<u16> {
    snapshot.get(address)
}

fn double(snapshot: &Snapshot, address: u16) -> Option<u32> {
    let high = word(snapshot, address)?;
    let low = word(snapshot, address.checked_add(1)?)?;
    Some(u32::from(high) << 16 | u32::from(low))
}

/// 1234 with 2 decimals is `12.34`.
pub(crate) fn fixed(value: i64, decimals: u32) -> String {
    let sign = if value < 0 { "-" } else { "" };
    let magnitude = value.unsigned_abs();
    if decimals == 0 {
        return format!("{sign}{magnitude}");
    }
    let scale = 10u64.saturating_pow(decimals);
    let (whole, fraction) = (magnitude / scale, magnitude % scale);
    format!("{sign}{whole}.{fraction:0width$}", width = decimals as usize)
}

#[derive(Clone, Copy)]
pub(crate) enum Num {
    U16,
    I16,
    U32,
}

fn raw(snapshot: &Snapshot, address: u16, kind: Num) -> Option<i64> {
    Some(match kind {
        Num::U16 => i64::from(word(snapshot, address)?),
        Num::I16 => i64::from(word(snapshot, address)? as i16),
        Num::U32 => i64::from(double(snapshot, address)?),
    })
}

pub(crate) fn number(snapshot: &Snapshot, address: u16, kind: Num, decimals: u32) -> Reading {
    Some(Ok(fixed(raw(snapshot, address, kind)?, decimals)))
}

/// A power in units of 10 W. Negated unless the inverter already has it positive `into_system`.
pub(crate) fn watts(snapshot: &Snapshot, address: u16, kind: Num, into_system: bool) -> Reading {
    let watts = raw(snapshot, address, kind)? * 10;
    Some(Ok(fixed(if into_system { watts } else { -watts }, 0)))
}

pub(crate) fn percentage(snapshot: &Snapshot, address: u16) -> Reading {
    let percent = word(snapshot, address)?;
    Some(if percent > 100 { Err(Invalid("more than 100 %")) } else { Ok(percent.to_string()) })
}

pub(crate) fn of_phase(first: u16, stride: u16, phase: u16) -> u16 {
    first + stride * phase
}

pub(crate) fn of_pv_input(first: u16, input: u16) -> u16 {
    first + PV_INPUT_REGISTERS * input
}

pub(crate) fn of_battery(first: u16, battery: u16) -> u16 {
    first + BATTERY_REGISTERS * battery
}

pub(crate) fn pv_power(snapshot: &Snapshot, equipment: Equipment) -> Reading {
    let powers = (0..equipment.pv_inputs).map(|i| word(snapshot, of_pv_input(reg::PV_POWER, i)));
    let total: Option<i64> = powers.map(|power| power.map(i64::from)).sum();
    Some(Ok(fixed(total? * 10, 0)))
}

pub(crate) const STATES: [&str; 8] = [
    "waiting",
    "checking",
    "on-grid",
    "off-grid",
    "fault",
    "permanent-fault",
    "updating",
    "self-charging",
];

pub(crate) fn state(snapshot: &Snapshot) -> Reading {
    let state = STATES.get(usize::from(word(snapshot, reg::STATE)?));
    Some(state.map(|state| state.to_string()).ok_or(Invalid("not a known state")))
}

/// SUBTLE: the bytes of each word are in the order of the codes, high byte first.
#[rustfmt::skip]
const FAULT_NAMES: [[&str; 16]; 12] = [
    ["GridOVP", "GridUVP", "GridOFP", "GridUFP", "GFCI", "OVRT", "LVRT", "IslandFault",
     "GridOVPInstant1", "GridOVPInstant2", "VGridLineFault", "InvOVP", "", "", "", ""],
    ["HwADFaultIGrid", "HwADFaultDCI(AC)", "HwADFaultVGrid(DC)", "HwADFaultVGrid(AC)",
     "GFCIDeviceFault(DC)", "GFCIDeviceFault(AC)", "HwADFaultDCV", "HwADFaultIdc",
     "HwADFaultDCI(DC)", "HwADFaultIdcBranch", "", "", "ConsistentFault_GFCI",
     "ConsistentFault_Vgrid", "ConsistentFault_DCI", ""],
    ["SpiCommFault(DC)", "SpiCommFault(AC)", "SChip_Fault", "MChip_Fault", "HwAuxPowerFault",
     "InverterSoftStartFail", "", "", "RelayFail", "IsoFault", "PEConnectFault", "PvConfigError",
     "CTDisconnect", "ReversalConnection", "ParallelFault", "SNTypeFault"],
    ["TempFault_Bat", "TempFault_HeatSink1", "TempFault_HeatSink2", "TempFault_HeatSink3",
     "TempFault_HeatSink4", "TempFault_HeatSink5", "TempFault_HeatSink6", "", "TempFault_Env1",
     "TempFault_Env2", "TempFault_Inv1", "TempFault_Inv2", "TempFault_Inv3", "", "", ""],
    ["VbusRmsUnbalance", "VbusInstantUnbalance", "BusUVP", "BusZVP", "PVOVP", "BatOVP",
     "LLCBusOVP", "SwBusRmsOVP", "SwBusInstantOVP", "FlyingCapOVP", "", "", "", "", "", ""],
    ["SwBatOCP", "DciOCP", "SwOCPInstant", "SwBuckBoostOCP", "SwAcRmsOCP", "SwPvOCPInstant",
     "IpvUnbalance", "IacUnbalance", "", "", "", "", "", "", "", ""],
    ["HwLLCBusOVP", "HwBusOVP", "HwBuckBoostOCP", "HwBatOCP", "", "HwPVOCP", "HwACOCP", "", "",
     "", "", "", "", "Overload1", "Overload2", "Overload3"],
    ["OverTempDerating", "FreqDerating", "FreqLoading", "VoltDerating", "VoltLoading", "", "", "",
     "", "", "", "BatLowVoltageAlarm", "BatLowVoltageShut", "BatLowVoltagePreAlarm", "", ""],
    ["unrecoverHwAcOCP", "unrecoverBusOVP", "unrecoverHwBusOVP", "unrecoverIpvUnbalance",
     "unrecoverEPSBatOCP", "unrecoverAcOCPInstant", "unrecoverIacUnbalance", "",
     "unrecoverPvConfigError", "unrecoverPVOCPInstant", "unrecoverHwPVOCP", "unrecoverRelayFail",
     "unrecoverVbusUnbalance", "unrecoverSpdFail(DC)", "unrecoverSpdFail(AC)", ""],
    ["USBFault", "WifiFault", "BluetoothFault", "RTCFault", "CommEEPROMFault", "FlashFault", "",
     "SafetyVerFault", "SciCommLose(DC)", "SciCommLose(AC)", "SciCommLose(Fuse)", "SoftVerError",
     "BMS1CommunicatonFault", "BMS2CommunicatonFault", "BMS3CommunicatonFault",
     "BMS4CommunicatonFault"],
    ["ForceShutdown", "RemoteShutdown", "Drms0Shutdown", "", "RemoteDerating",
     "LogicInterfaceDerating", "AlarmAntiRefluxing", "", "FanFault1", "FanFault2", "FanFault3",
     "FanFault4", "FanFault5", "FanFault6", "", ""],
    ["BmsOVP", "BmsUVP", "BmsOTP", "BmsUTP", "BmsOCP", "BmsShort", "", "", "", "", "", "", "", "",
     "", ""],
];

/// As `ID<code> <name>`, the code being what the display shows.
pub(crate) fn faults(snapshot: &Snapshot) -> Reading {
    let mut active = Vec::new();
    for (index, names) in (0u16..).zip(&FAULT_NAMES) {
        let bits = word(snapshot, reg::FAULTS + index)?.swap_bytes();
        for (bit, name) in (0u16..).zip(names) {
            if bits & (1 << bit) != 0 {
                let code = index * 16 + bit + 1;
                active.push(if name.is_empty() {
                    format!("ID{code:03}")
                } else {
                    format!("ID{code:03} {name}")
                });
            }
        }
    }
    Some(serde_json::to_string(&active).map_err(|_| Invalid("cannot be made into JSON")))
}

fn text(snapshot: &Snapshot, start: u16, count: u16) -> Reading {
    let mut bytes = Vec::with_capacity(usize::from(count) * 2);
    for i in 0..count {
        bytes.extend(word(snapshot, start.checked_add(i)?)?.to_be_bytes());
    }
    let length = bytes.iter().rposition(|&b| b != 0 && b != b' ').map_or(0, |last| last + 1);
    bytes.truncate(length);
    Some(if bytes.is_empty() {
        Err(Invalid("empty"))
    } else if !bytes.iter().all(|b| (0x20..0x7f).contains(b)) {
        Err(Invalid("not printable ASCII"))
    } else {
        Ok(bytes.into_iter().map(char::from).collect())
    })
}

pub(crate) fn serial_number(snapshot: &Snapshot) -> Reading {
    text(snapshot, reg::SERIAL_NUMBER, 7)
}

pub(crate) fn hardware_version(snapshot: &Snapshot) -> Reading {
    text(snapshot, reg::HARDWARE_VERSION, 2)
}

/// Formatted like the firmware files are named, as in `V120005`.
pub(crate) fn software_version(snapshot: &Snapshot, address: u16) -> Reading {
    let stage = char::from(word(snapshot, address)?.to_be_bytes()[1]);
    let rest = text(snapshot, address.checked_add(1)?, 3)?;
    Some(rest.and_then(|rest| match stage {
        stage if stage.is_ascii_alphanumeric() => Ok(format!("{stage}{rest}")),
        _ => Err(Invalid("the stage is not a letter")),
    }))
}

/// Two digits in each word, as in `10.08`.
pub(crate) fn safety_version(snapshot: &Snapshot) -> Reading {
    let major = text(snapshot, reg::SAFETY_VERSION, 1)?;
    let minor = text(snapshot, reg::SAFETY_VERSION + 1, 1)?;
    Some(major.and_then(|major| Ok(format!("{major}.{}", minor?))))
}

fn clock_time(snapshot: &Snapshot) -> Option<Result<jiff::civil::DateTime, Invalid>> {
    let mut fields = [0; 6];
    for (i, field) in (0..).zip(&mut fields) {
        *field = word(snapshot, reg::CLOCK + i)?;
    }
    let [year, month, day, hour, minute, second] = fields;
    // Only two digits can be written, but the read-back is not documented.
    let year = if year < 100 { year + 2000 } else { year };
    let small = |value: u16| i8::try_from(value).map_err(|_| Invalid("not a valid time"));
    Some((|| {
        let year = i16::try_from(year).map_err(|_| Invalid("not a valid time"))?;
        let (month, day) = (small(month)?, small(day)?);
        let (hour, minute, second) = (small(hour)?, small(minute)?, small(second)?);
        jiff::civil::DateTime::new(year, month, day, hour, minute, second, 0)
            .map_err(|_| Invalid("not a valid time"))
    })())
}

pub(crate) fn clock(snapshot: &Snapshot) -> Reading {
    Some(clock_time(snapshot)?.map(|time| time.strftime("%Y-%m-%dT%H:%M:%S").to_string()))
}

pub(crate) fn clock_write(time: jiff::civil::DateTime) -> Result<Write, Invalid> {
    let year = time.year() - 2000;
    if !(0..100).contains(&year) {
        return Err(Invalid("the inverter can only be set to a year from 2000 to 2099"));
    }
    let fields = [year, time.month().into(), time.day().into()];
    let fields =
        fields.into_iter().chain([time.hour(), time.minute(), time.second()].map(i16::from));
    let words = fields.map(|field| field as u16).chain([1]).collect();
    Ok(Write { start: reg::CLOCK_SETTING, words })
}

pub(crate) const SYNC_CLOCK: [&str; 1] = ["now"];

pub(crate) fn sync_clock(_: &Snapshot, value: &str) -> Result<Write, Invalid> {
    choose(value, &SYNC_CLOCK)?;
    clock_write(jiff::Zoned::now().datetime())
}

/// The inverter keeps local time without a zone, so an instant is converted into `zone`.
fn parse_clock(value: &str, zone: &jiff::tz::TimeZone) -> Result<jiff::civil::DateTime, Invalid> {
    match value.parse::<jiff::Timestamp>() {
        Ok(instant) => Ok(zone.to_datetime(instant)),
        Err(_) => value.parse().map_err(|_| Invalid("not a date and a time")),
    }
}

pub(crate) fn set_clock(_: &Snapshot, value: &str) -> Result<Write, Invalid> {
    clock_write(parse_clock(value, &jiff::tz::TimeZone::system())?)
}

fn choice(snapshot: &Snapshot, address: u16, values: &[&str]) -> Reading {
    let value = values.get(usize::from(word(snapshot, address)?));
    Some(value.map(|value| value.to_string()).ok_or(Invalid("not one of the known values")))
}

fn choose(value: &str, values: &[&str]) -> Result<u16, Invalid> {
    let index = values.iter().position(|v| *v == value).ok_or(Invalid("not one of the values"))?;
    Ok(index as u16)
}

pub(crate) const STORAGE_MODES: [&str; 5] =
    ["self-use", "time-of-use", "timing", "passive", "peak-shaving"];

pub(crate) fn storage_mode(snapshot: &Snapshot) -> Reading {
    choice(snapshot, reg::STORAGE_MODE, &STORAGE_MODES)
}

pub(crate) fn set_storage_mode(_: &Snapshot, value: &str) -> Result<Write, Invalid> {
    Ok(Write { start: reg::STORAGE_MODE, words: vec![choose(value, &STORAGE_MODES)?] })
}

pub(crate) const EPS_MODES: [&str; 3] = ["disabled", "enabled", "enabled-with-cold-start"];

pub(crate) fn eps_mode(snapshot: &Snapshot) -> Reading {
    choice(snapshot, reg::EPS_CONTROL, &EPS_MODES)
}

/// The EPS control has to be written together with the wait time that follows it.
pub(crate) fn set_eps_mode(snapshot: &Snapshot, value: &str) -> Result<Write, Invalid> {
    let mode = choose(value, &EPS_MODES)?;
    let wait_time = word(snapshot, reg::EPS_WAIT_TIME).unwrap_or(0);
    Ok(Write { start: reg::EPS_CONTROL, words: vec![mode, wait_time] })
}

pub(crate) fn enabled(snapshot: &Snapshot) -> Reading {
    Some(match word(snapshot, reg::REMOTE_SWITCH)? {
        raw @ (0 | 1) => Ok((raw == 1).to_string()),
        _ => Err(Invalid("neither 0 nor 1")),
    })
}

pub(crate) fn set_enabled(_: &Snapshot, value: &str) -> Result<Write, Invalid> {
    let on = match value {
        "true" => 1,
        "false" => 0,
        _ => return Err(Invalid("neither true nor false")),
    };
    Ok(Write { start: reg::REMOTE_SWITCH, words: vec![on] })
}

/// Which of the sets of parameters the passive mode follows. Only those of `manual` are exposed.
pub(crate) const PASSIVE_MODES: [&str; 3] = ["auto", "manual", "scheduler"];

pub(crate) fn passive_mode(snapshot: &Snapshot) -> Reading {
    choice(snapshot, reg::PASSIVE_MODE, &PASSIVE_MODES)
}

pub(crate) fn set_passive_mode(_: &Snapshot, value: &str) -> Result<Write, Invalid> {
    Ok(Write { start: reg::PASSIVE_MODE, words: vec![choose(value, &PASSIVE_MODES)?] })
}

pub(crate) const TIMEOUT_ACTIONS: [&str; 2] = ["standby", "previous-mode"];

pub(crate) fn passive_timeout_action(snapshot: &Snapshot) -> Reading {
    choice(snapshot, reg::PASSIVE_TIMEOUT_ACTION, &TIMEOUT_ACTIONS)
}

pub(crate) fn set_passive_timeout_action(_: &Snapshot, value: &str) -> Result<Write, Invalid> {
    let action = choose(value, &TIMEOUT_ACTIONS)?;
    Ok(Write { start: reg::PASSIVE_TIMEOUT_ACTION, words: vec![action] })
}

pub(crate) fn set_passive_timeout(_: &Snapshot, value: &str) -> Result<Write, Invalid> {
    let seconds = value.parse::<u16>().map_err(|_| Invalid("not a number of seconds"))?;
    Ok(Write { start: reg::PASSIVE_TIMEOUT, words: vec![seconds] })
}

pub(crate) fn passive_power(snapshot: &Snapshot, address: u16) -> Reading {
    Some(Ok((double(snapshot, address)? as i32).to_string()))
}

pub(crate) fn set_passive_power(address: u16, value: &str) -> Result<Write, Invalid> {
    let watts = value.parse::<i32>().map_err(|_| Invalid("not a whole number of watts"))?;
    let [high, low] = [(watts >> 16) as u16, watts as u16];
    Ok(Write { start: address, words: vec![high, low] })
}

pub(crate) fn is_complete(snapshot: &Snapshot) -> bool {
    let needed = [IDENTIFICATION, INPUT_TYPES, STATUS, GRID, EPS_OUTPUT, ENERGY, PV, BATTERIES];
    needed.into_iter().all(|Block { start, count }| snapshot.words(start, count).is_some())
}

/// Each input channel is unused (0), a PV input (1 to 127) or a battery input (128 to 255).
/// Channels of the same number are in parallel, as one input.
pub(crate) fn equipment(snapshot: &Snapshot) -> Option<Result<Equipment, Invalid>> {
    let types = snapshot.words(INPUT_TYPES.start, INPUT_TYPES.count)?;
    let count = |range: std::ops::RangeInclusive<u16>| {
        types.iter().filter(|t| range.contains(t)).collect::<BTreeSet<_>>().len() as u16
    };
    let (pv_inputs, batteries) = (count(1..=127), count(128..=255));
    Some(if pv_inputs > MAX_PV_INPUTS {
        Err(Invalid("more PV inputs than the inverter has registers for"))
    } else if batteries > MAX_BATTERIES {
        Err(Invalid("more batteries than the inverter has registers for"))
    } else {
        Ok(Equipment { pv_inputs, batteries })
    })
}

#[cfg(test)]
fn polled() -> BTreeSet<u16> {
    let plan = poll_plan(Duration::from_secs(1));
    plan.iter().flat_map(|block| block.start..block.start + block.count).collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) const EQUIPMENT: Equipment = Equipment { pv_inputs: 2, batteries: 1 };

    fn words_of(text: &[u8]) -> Vec<u16> {
        text.chunks(2).map(|c| u16::from_be_bytes([c[0], *c.get(1).unwrap_or(&0)])).collect()
    }

    pub(crate) fn fixture() -> Snapshot {
        let mut snapshot = Snapshot::new();
        let mut set = |address: u16, words: &[u16]| {
            snapshot.set_many(address, words);
        };
        let mut status = [0u16; STATUS.count as usize];
        status[0] = 2;
        status[1] = 0x0100;
        status[(0x040F - 0x0404) as usize] = 0x1000;
        status[(0x0418 - 0x0404) as usize] = 31;
        status[(0x041A - 0x0404) as usize] = 45;
        status[(0x0420 - 0x0404) as usize] = 0xFFFE;
        status[(0x042B - 0x0404) as usize] = 1900;
        status[(0x042C - 0x0404) as usize..].copy_from_slice(&[26, 10, 27, 22, 47, 5]);
        set(STATUS.start, &status);
        let mut identification = [0u16; IDENTIFICATION.count as usize];
        let serial = words_of(b"SP1ES120N12345");
        identification[..7].copy_from_slice(&serial);
        identification[8..10].copy_from_slice(&words_of(b"V100"));
        let version = [u16::from(b'V')].into_iter().chain(words_of(b"120005"));
        let version: Vec<u16> = version.collect();
        identification[10..14].copy_from_slice(&version);
        identification[14..18].copy_from_slice(&version);
        identification[18..22].copy_from_slice(&version);
        identification[22..24].copy_from_slice(&words_of(b"1008"));
        set(IDENTIFICATION.start, &identification);
        let mut grid = [0u16; GRID.count as usize];
        let mut put = |address: u16, words: &[u16]| {
            let at = usize::from(address - GRID.start);
            grid[at..at + words.len()].copy_from_slice(words);
        };
        put(0x0484, &[5001, 250, 12, 251, (-30i16) as u16, 3, 31]);
        for phase in 0..3 {
            let first = of_phase(reg::PHASE_VOLTAGE, reg::PHASE_STRIDE, phase);
            put(first, &[2301 + phase, 362, 83 + phase, (-4i16) as u16, 998]);
        }
        put(0x04AE, &[0, 280]);
        set(GRID.start, &grid);
        let mut eps = [0u16; EPS_OUTPUT.count as usize];
        eps[..4].copy_from_slice(&[0, 0, 0, 5000]);
        eps[6] = 2299;
        eps[11] = 12;
        set(EPS_OUTPUT.start, &eps);
        set(0x0584, &[5503, 512, 282, 4802, 401, 192]);
        set(0x0604, &[4012, 375, 150, 24, 67, 99, 123]);
        set(ENERGY.start, &[0, 1234, 0, 56789, 0, 801, 0, 40123, 0, 52, 0, 7031, 0, 3, 0, 9876]);
        set(0x0694, &[0, 412, 0, 21002, 0, 380, 0, 19807]);
        set(INPUT_TYPES.start, &[1, 2, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        set(0x058A, &[0; 6]);
        set(0x060B, &[0; 21]);
        set(0x1029, &[1, 0]);
        set(0x1104, &[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3]);
        let mut passive = [0u16; PASSIVE.count as usize];
        passive[..13]
            .copy_from_slice(&[600, 1, 0, 0, 500, 0xFFFF, 0xFE0C, 0, 10000, 0, 0, 0, 5000]);
        passive[PASSIVE.count as usize - 1] = 1;
        set(PASSIVE.start, &passive);
        snapshot
    }

    fn ok(reading: Reading) -> String {
        reading.expect("has been read").expect("is valid")
    }

    fn invalid(reading: Reading) -> &'static str {
        reading.expect("has been read").expect_err("is not valid").0
    }

    #[test]
    fn numbers_are_written_with_all_of_their_decimals() {
        for (value, decimals, text) in [
            (3833, 1, "383.3"),
            (-11, 2, "-0.11"),
            (0, 2, "0.00"),
            (5, 3, "0.005"),
            (-12345, 0, "-12345"),
        ] {
            assert_eq!(fixed(value, decimals), text);
        }
    }

    #[test]
    fn the_fixture_is_decoded() {
        let s = fixture();
        assert!(is_complete(&s));
        assert_eq!(equipment(&s), Some(Ok(EQUIPMENT)));
        assert_eq!(ok(state(&s)), "on-grid");
        assert_eq!(ok(faults(&s)), r#"["ID001 GridOVP","ID165 RemoteDerating"]"#);
        assert_eq!(ok(number(&s, reg::INTERNAL_TEMPERATURE, Num::I16, 0)), "-2");
        assert_eq!(ok(clock(&s)), "2026-10-27T22:47:05");
        assert_eq!(ok(serial_number(&s)), "SP1ES120N12345");
        assert_eq!(ok(hardware_version(&s)), "V100");
        assert_eq!(ok(software_version(&s, reg::COMMUNICATION_VERSION)), "V120005");
        assert_eq!(ok(software_version(&s, reg::SECONDARY_VERSION)), "V120005");
        assert_eq!(ok(safety_version(&s)), "10.08");
        assert_eq!(ok(number(&s, reg::GRID_FREQUENCY, Num::U16, 2)), "50.01");
        assert_eq!(ok(watts(&s, reg::GRID_POWER, Num::I16, false)), "300");
        assert_eq!(ok(watts(&s, reg::LOAD_POWER, Num::U16, true)), "2800");
        let l3_voltage = of_phase(reg::PHASE_VOLTAGE, reg::PHASE_STRIDE, 2);
        assert_eq!(ok(number(&s, l3_voltage, Num::U16, 1)), "230.3");
        assert_eq!(ok(pv_power(&s, EQUIPMENT)), "4740");
        let pv2_voltage = of_pv_input(reg::PV_VOLTAGE, 1);
        assert_eq!(ok(number(&s, pv2_voltage, Num::U16, 1)), "480.2");
        assert_eq!(ok(watts(&s, reg::BATTERY_POWER, Num::I16, true)), "1500");
        assert_eq!(ok(percentage(&s, reg::BATTERY_STATE_OF_CHARGE)), "67");
        assert_eq!(ok(number(&s, reg::PV_GENERATION_TODAY, Num::U32, 2)), "12.34");
        assert_eq!(ok(number(&s, reg::PV_GENERATION_TOTAL, Num::U32, 1)), "5678.9");
        assert_eq!(ok(eps_mode(&s)), "enabled");
        assert_eq!(ok(enabled(&s)), "true");
        assert_eq!(ok(storage_mode(&s)), "passive");
        assert_eq!(ok(passive_timeout_action(&s)), "previous-mode");
        assert_eq!(ok(passive_power(&s, reg::PASSIVE_GRID_POWER)), "500");
        assert_eq!(ok(passive_power(&s, reg::PASSIVE_BATTERY_MIN_POWER)), "-500");
        assert_eq!(ok(passive_power(&s, reg::PASSIVE_BATTERY_MAX_POWER)), "10000");
        assert_eq!(ok(passive_power(&s, reg::PASSIVE_EXPORT_LIMIT)), "0");
        assert_eq!(ok(passive_power(&s, reg::PASSIVE_IMPORT_LIMIT)), "5000");
        assert_eq!(ok(passive_mode(&s)), "manual");
    }

    #[test]
    fn nothing_is_made_up_for_registers_not_read() {
        let s = Snapshot::new();
        assert!(!is_complete(&s));
        assert_eq!(equipment(&s), None);
        assert_eq!(state(&s), None);
        assert_eq!(faults(&s), None);
        assert_eq!(clock(&s), None);
        assert_eq!(pv_power(&s, EQUIPMENT), None);
        assert_eq!(software_version(&s, reg::PRIMARY_VERSION), None);
    }

    #[test]
    fn nonsense_is_invalid() {
        let mut s = fixture();
        s.set(reg::STATE, 8);
        assert_eq!(invalid(state(&s)), "not a known state");
        s.set(reg::BATTERY_STATE_OF_CHARGE, 101);
        assert_eq!(invalid(percentage(&s, reg::BATTERY_STATE_OF_CHARGE)), "more than 100 %");
        s.set(reg::CLOCK + 1, 13);
        assert_eq!(invalid(clock(&s)), "not a valid time");
        s.set(reg::REMOTE_SWITCH, 2);
        assert_eq!(invalid(enabled(&s)), "neither 0 nor 1");
        s.set(reg::STORAGE_MODE, 5);
        assert_eq!(invalid(storage_mode(&s)), "not one of the known values");
        s.set_many(reg::SERIAL_NUMBER, &[0; 7]);
        assert_eq!(invalid(serial_number(&s)), "empty");
        s.set(reg::SERIAL_NUMBER, 0x0102);
        assert_eq!(invalid(serial_number(&s)), "not printable ASCII");
    }

    #[test]
    fn unnamed_faults_are_known_by_their_code() {
        let mut s = fixture();
        s.set_many(reg::FAULTS, &[0x0010, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x0200, 0x0180]);
        let active = ok(faults(&s));
        assert_eq!(active, r#"["ID013","ID162 RemoteShutdown","ID177 BmsOVP","ID192"]"#);
        s.set_many(reg::FAULTS, &[0; 12]);
        assert_eq!(ok(faults(&s)), "[]");
    }

    #[test]
    fn what_to_write() {
        let s = fixture();
        let write = |start, words: &[u16]| Ok(Write { start, words: words.to_vec() });
        assert_eq!(set_storage_mode(&s, "self-use"), write(0x1110, &[0]));
        assert_eq!(set_storage_mode(&s, "peak-shaving"), write(0x1110, &[4]));
        assert!(set_storage_mode(&s, "Passive").is_err());
        assert_eq!(set_enabled(&s, "false"), write(0x1104, &[0]));
        assert_eq!(set_enabled(&s, "true"), write(0x1104, &[1]));
        assert!(set_enabled(&s, "1").is_err());
        assert_eq!(set_passive_timeout(&s, "600"), write(0x1184, &[600]));
        assert!(set_passive_timeout(&s, "65536").is_err());
        assert!(set_passive_timeout(&s, "-1").is_err());
        assert_eq!(set_passive_timeout_action(&s, "standby"), write(0x1185, &[0]));
        assert_eq!(set_passive_mode(&s, "scheduler"), write(0x11A9, &[2]));
        assert!(set_passive_mode(&s, "Manual").is_err());
        assert_eq!(set_passive_power(0x1187, "500"), write(0x1187, &[0, 500]));
        assert_eq!(set_passive_power(0x1189, "-500"), write(0x1189, &[0xFFFF, 0xFE0C]));
        assert_eq!(set_passive_power(0x118B, "100000"), write(0x118B, &[1, 0x86A0]));
        assert!(set_passive_power(0x1187, "1.5").is_err());
        assert!(set_passive_power(0x1187, "3000000000").is_err());
    }

    #[test]
    fn the_equipment_is_what_the_input_channels_are_set_up_as() {
        let mut s = Snapshot::new();
        let mut with = |types: &[u16]| {
            let mut words = [0; INPUT_TYPES.count as usize];
            words[..types.len()].copy_from_slice(types);
            s.set_many(INPUT_TYPES.start, &words);
            equipment(&s)
        };
        let equipped = |pv_inputs, batteries| Some(Ok(Equipment { pv_inputs, batteries }));
        assert_eq!(with(&[]), equipped(0, 0));
        assert_eq!(with(&[1, 2, 128]), equipped(2, 1));
        // In parallel.
        assert_eq!(with(&[1, 1, 128, 128]), equipped(1, 1));
        assert_eq!(with(&[1, 2, 3, 4, 128, 129, 130, 255]), equipped(4, 4));
        assert!(with(&[1, 2, 3, 4, 5]).unwrap().is_err());
        assert!(with(&[128, 129, 130, 131, 132]).unwrap().is_err());
    }

    #[test]
    fn writes_are_taken_if_they_read_back_or_commit() {
        let eps = Write { start: 0x1029, words: vec![1, 30] };
        assert_eq!(eps.taken(&[1, 30]), Ok(()));
        assert!(eps.taken(&[0, 30]).is_err());
        assert!(eps.taken(&[1]).is_err());
        let clock = clock_write("2026-10-27T22:47:05".parse().unwrap()).unwrap();
        assert_eq!(clock.taken(&[26, 10, 27, 22, 47, 5, 0]), Ok(()));
        assert_eq!(clock.taken(&[26, 10, 27, 22, 47, 5, 1]), Ok(()));
        assert!(clock.taken(&[26, 10, 27, 22, 47, 5, 0xFFFB]).is_err());
        assert!(clock.taken(&[26, 10, 27, 22, 48, 5, 0]).is_err());
    }

    #[test]
    fn the_eps_mode_is_written_with_the_wait_time() {
        let mut s = fixture();
        let write = |words: &[u16]| Ok(Write { start: 0x1029, words: words.to_vec() });
        assert_eq!(set_eps_mode(&s, "disabled"), write(&[0, 0]));
        assert_eq!(set_eps_mode(&s, "enabled-with-cold-start"), write(&[2, 0]));
        s.set(reg::EPS_WAIT_TIME, 30);
        assert_eq!(set_eps_mode(&s, "enabled"), write(&[1, 30]));
        assert_eq!(set_eps_mode(&Snapshot::new(), "enabled"), write(&[1, 0]));
        assert!(set_eps_mode(&s, "on").is_err());
    }

    #[test]
    fn the_clock_is_set_with_two_digits_of_the_year_and_then_taken() {
        let s = Snapshot::new();
        let write = |words: &[u16]| Ok(Write { start: 0x1004, words: words.to_vec() });
        assert_eq!(set_clock(&s, "2026-10-27T22:47:05"), write(&[26, 10, 27, 22, 47, 5, 1]));
        assert!(set_clock(&s, "2100-01-01T00:00:00").is_err());
        assert!(set_clock(&s, "1999-12-31T23:59:59").is_err());
        assert!(set_clock(&s, "yesterday").is_err());

        let zone = jiff::tz::TimeZone::fixed(jiff::tz::offset(2));
        let local = |value| parse_clock(value, &zone).unwrap().to_string();
        assert_eq!(local("2026-10-27T20:47:05Z"), "2026-10-27T22:47:05");
        assert_eq!(local("2026-10-27T21:47:05+01:00"), "2026-10-27T22:47:05");
        assert_eq!(local("2026-10-27T22:47:05"), "2026-10-27T22:47:05");
    }

    #[test]
    fn the_poll_plan_covers_everything() {
        let polled = polled();
        for address in [0x0404, 0x0431, 0x0445, 0x045C, 0x0484, 0x04AF, 0x0504, 0x051F] {
            assert!(polled.contains(&address), "{address:#06x}");
        }
        assert!(polled.contains(&0x058F) && !polled.contains(&0x0590));
        assert!(polled.contains(&0x061F) && !polled.contains(&0x0620));
        for address in [0x069B, 0x1011, 0x1020, 0x1029, 0x102A, 0x1104, 0x1110, 0x1184, 0x11A9] {
            assert!(polled.contains(&address), "{address:#06x}");
        }
        let plan = poll_plan(Duration::from_secs(4));
        let period = |start| plan.iter().find(|b| b.start == start).unwrap().period;
        assert_eq!(period(0x0404), Duration::from_secs(4));
        assert_eq!(period(0x0684), Duration::from_secs(15));
        assert_eq!(period(0x1104), Duration::from_secs(30));
        assert_eq!(period(0x0445), Duration::from_secs(600));
    }
}
