<results> {
    for $p in doc('expdb/auction.xml')/site/people/person
    let $e := $p/homepage
    where count($e) = 0
    return
        <person_without_homepage>
            {$p/name}
        </person_without_homepage>
} </results>
