<results> {
    for $it in collection('auction')/site/regions/samerica/item
    for $pe in collection('auction')/site/people/person
    where $pe/profile/interest/@category = $it/incategory/@category
    return
    <people>
        {$pe}
    </people>
} </results>
