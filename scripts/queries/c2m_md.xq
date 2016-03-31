<results> {
    for $cat in collection('auction')/site/categories/category
    for $pe in collection('auction')/site/people/person
    where count($pe/profile/interest) > 3 and $pe/profile/interest/@category = $cat/@id
    return
        <match>
            <person>{$pe/name}</person>
            <category>{$cat/name}</category>
        </match>
} </results>
