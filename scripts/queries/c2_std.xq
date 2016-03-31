<results> {
    for $pe in doc('expdb/auction.xml')/site/people/person
    for $cat in doc('expdb/auction.xml')/site/categories/category
    where count($pe/profile/interest) > 3 and $pe/profile/interest/@category = $cat/@id
    return
        <match>
            <person>{$pe/name}</person>
            <category>{$cat/name}</category>
        </match>
} </results>
