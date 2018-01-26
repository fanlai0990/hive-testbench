graph [
    label "simple"
    node [
        id          1
        label       "n1"
        Longitude   1
        Latitude    3
    ]
    node [
        id          2
        label       "n2"
        Longitude   3
        Latitude    5
    ]
    node [
        id          3
        label       "n3"
        Longitude   3
        Latitude    1
    ]
    node [
        id          4
        label       "n4"
        Longitude   5
        Latitude    3
    ]
    edge [
        source      1
        target      2
        bandwidth   20
    ]
    edge [
        source      1
        target      3
        bandwidth   10
    ]
    edge [
        source      2
        target      4
        bandwidth   10
    ]
    edge [
        source      3
        target      4
        bandwidth   20
    ]
    edge [
        source      2
        target      3
        bandwidth   20
    ]
]