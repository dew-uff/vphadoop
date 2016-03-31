<results> {
    for $pe in doc('auction.xml')/site/people/person
    for $cat in doc('auction.xml')/site/categories/category
    where count($pe/profile/interest) > 3 and $pe/profile/interest/@category = $cat/@id
    return
        <match>
            <person>{$pe/name}</person>
            <category>{$cat/name}</category>
        </match>
} </results>
