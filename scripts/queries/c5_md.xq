<results> {
    for $p in collection('auction')/site/people/person
    where $p/profile/@income > 10000
    return
        <people>
            {$p}
        </people>
} </results>
