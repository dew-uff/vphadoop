<results> {
    for $pe in collection('auction')/site/people/person
    let $int := $pe/profile/interest
    where $pe/profile/business = "Yes" and count($int) > 1
    order by $pe/name
    return
    <person>
        {$pe}
    </person>
} </results>
