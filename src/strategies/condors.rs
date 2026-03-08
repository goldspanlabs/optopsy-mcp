use super::helpers::{
    call_leg, default_deep_itm_delta, default_deep_otm_delta, default_itm_delta, default_otm_delta,
    put_leg, strategy, Side, StrategyDef,
};

pub fn long_call_condor() -> StrategyDef {
    strategy(
        "long_call_condor",
        "Condors",
        "Buy 1 lowest call, sell 1 lower-mid call, sell 1 upper-mid call, buy 1 highest call",
        vec![
            call_leg(Side::Long, 1, default_deep_otm_delta()),  // lowest strike
            call_leg(Side::Short, 1, default_otm_delta()),      // lower-mid strike
            call_leg(Side::Short, 1, default_itm_delta()),      // upper-mid strike
            call_leg(Side::Long, 1, default_deep_itm_delta()),  // highest strike
        ],
    )
}

pub fn short_call_condor() -> StrategyDef {
    strategy(
        "short_call_condor",
        "Condors",
        "Sell 1 lowest call, buy 1 lower-mid call, buy 1 upper-mid call, sell 1 highest call",
        vec![
            call_leg(Side::Short, 1, default_deep_otm_delta()), // lowest strike
            call_leg(Side::Long, 1, default_otm_delta()),       // lower-mid strike
            call_leg(Side::Long, 1, default_itm_delta()),       // upper-mid strike
            call_leg(Side::Short, 1, default_deep_itm_delta()), // highest strike
        ],
    )
}

pub fn long_put_condor() -> StrategyDef {
    strategy(
        "long_put_condor",
        "Condors",
        "Buy 1 lowest put, sell 1 lower-mid put, sell 1 upper-mid put, buy 1 highest put",
        vec![
            put_leg(Side::Long, 1, default_deep_otm_delta()),  // lowest strike
            put_leg(Side::Short, 1, default_otm_delta()),      // lower-mid strike
            put_leg(Side::Short, 1, default_itm_delta()),      // upper-mid strike
            put_leg(Side::Long, 1, default_deep_itm_delta()),  // highest strike
        ],
    )
}

pub fn short_put_condor() -> StrategyDef {
    strategy(
        "short_put_condor",
        "Condors",
        "Sell 1 lowest put, buy 1 lower-mid put, buy 1 upper-mid put, sell 1 highest put",
        vec![
            put_leg(Side::Short, 1, default_deep_otm_delta()), // lowest strike
            put_leg(Side::Long, 1, default_otm_delta()),       // lower-mid strike
            put_leg(Side::Long, 1, default_itm_delta()),       // upper-mid strike
            put_leg(Side::Short, 1, default_deep_itm_delta()), // highest strike
        ],
    )
}

pub fn all() -> Vec<StrategyDef> {
    vec![
        long_call_condor(),
        short_call_condor(),
        long_put_condor(),
        short_put_condor(),
    ]
}
