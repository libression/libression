function renderImpression(file_key){
    existing_impression = document.getElementById(file_key);
    if (existing_impression) {
        return existing_impression;
    } else {
        return _fetchImpression(file_key);
    }
}

function _fetchImpression(file_key){
    img = new Image();
    img.src = window.location.href + "thumbnail/" + file_key;

    impression = document.createElement("figure");
    impression.id = file_key
    impression.appendChild(img)

    return impression
}

function refreshGallery(file_keys){
    new_inner_gallery = document.createElement("div")
    new_inner_gallery.className = "gallery"
    file_keys?.forEach(function (file_key) {
        impression = renderImpression(file_key)
        new_inner_gallery.appendChild(impression);
    });

    gallery = document.getElementsByClassName("gallery")
    if (gallery){
        gallery[0].replaceWith(new_inner_gallery);
    }
}

function _updateDirListener(dom, dir_str, prefix=""){
    dom.innerText = prefix.concat(dir_str)
    dom.addEventListener('click', function(){
        refreshPage(dir_str);
    });
    return dom
}

function refreshInnerDirs(inner_dirs){
    new_inner_dirs = document.createElement("div")
    new_inner_dirs.className = "dropdown-content"
    inner_dirs?.forEach(function (inner_dir) {
        inner_dir_elem = document.createElement("a");
        _updateDirListener(inner_dir_elem, inner_dir, "/")
        new_inner_dirs.appendChild(inner_dir_elem);
    });

    inner_dir_dom = document.getElementsByClassName("dropdown-content")
    if (inner_dir_dom){
        inner_dir_dom[0].replaceWith(new_inner_dirs)
    }
}

function refreshOuterDirs(outer_dir){
    dom = document.querySelector("#outer_dir")
    new_dom = dom.cloneNode(true);
    _updateDirListener(new_dom, outer_dir, "par_dir: /")
    dom.replaceWith(new_dom)  // clears old listeners
}

function _refreshCheckboxes(query_str, cur_dir){
    dom = document.querySelector(query_str)
    new_dom = dom.cloneNode(true);
    new_dom.addEventListener('click', function(){
        refreshPage(cur_dir);
    });
    dom.replaceWith(new_dom)  // clears old listeners
}

async function refreshPage(cur_dir=""){
    show_subdirs = document.querySelector("#show_subdirs").checked
    show_hidden_content = document.querySelector("#show_hidden_content").checked

    url = window.location.href + "refresh_page_params";
    payload = {
        "cur_dir": cur_dir,
        "show_subdirs": show_subdirs,
        "show_hidden_content": show_hidden_content
    }

    cur_dir_dom = document.querySelector("#cur_dir")
    cur_dir_dom.innerText = "cur_dir: /".concat(cur_dir)
    _refreshCheckboxes("#show_subdirs", cur_dir)
    _refreshCheckboxes("#show_hidden_content", cur_dir)

    response = await fetch(
        url, {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify(payload), // body data type must match "Content-Type" header
        }
    ).then(
        (response) => response.json()
    ).then(
        (data) => _updateDOM(data)
    );
};

function _updateDOM(response_dict){
    // Update DOM
    refreshGallery(response_dict["file_keys"])
    refreshInnerDirs(response_dict["inner_dirs"])
    refreshOuterDirs(response_dict["outer_dir"])
}
