<results> {
    for $p in doc('auction.xml')/site/people/person
    where $p/profile/@income > 10000
    return
        <people>
            {$p}
        </people>
} </results>
