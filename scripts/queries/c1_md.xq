<results> {
    for $it in collection('auction')/site/regions/samerica/item
    for $pe in collection('auction')/site/people/person
    where $pe/profile/@income > 90000 and $pe/profile/interest/@category = $it/incategory/@category
    return
    <match>
        <person>{$pe/name}</person>
        <item>{$it/name}</item>
    </match>
} </results>
