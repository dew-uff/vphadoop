<results> {
    for $it in collection('auction')/site/regions/samerica/item
    for $pe in collection('auction')/site/people/person
    where $it/incategory/@category = "category250" and $pe/profile/interest/@category = $it/incategory/@category and $pe/profile/education = "Graduate School"
    return
        <match_cat_250>
            {$pe}
        </match_cat_250>
} </results>
