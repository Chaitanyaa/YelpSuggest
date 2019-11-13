$(function() {
  
  var firebaseConfig = {
    apiKey: "AIzaSyCH2UIVC6pqVDLBgCA-dw9s_EwzP0cQlR8",
    authDomain: "commenting-app.firebaseapp.com",
    databaseURL: "https://commenting-app.firebaseio.com",
    projectId: "commenting-app",
    storageBucket: "",
    messagingSenderId: "488177845904",
    appId: "1:488177845904:web:287e67aeae0dab72"
  };
  // Initialize Firebase
  firebase.initializeApp(firebaseConfig);  
  
  var ref = new Firebase("https://commenting-app.firebaseio.com/"),
    
    postRef = ref.child(slugify(window.location.pathname));
    var i = 0;
    postRef.on("child_added", function(snapshot) {
      var newPost = snapshot.val();
      $(".comments").prepend('<div class="alert alert-dismissible alert-secondary"><div class="comment">' + '<table><tr><td rowspan="3">'+
        '<div class="profile-image" style="float:left"><img src="http://www.gravatar.com/avatar/'+escapeHtml(newPost.md5Hash)+'?s=100&d=retro"/></div></td> ' +
        '<td style="width:130%;"><h4 style="float:left;margin-left:10px;">' + escapeHtml(newPost.name) +
        '</h4></td><td rowspan="3">'+
        '<div><span>Predicted:'+escapeHtml(newPost.predicted)+'</span></br>'+
        '<span>Labeled:'+escapeHtml(newPost.labeled)+'</span></br>'+
        '<span>Score:'+escapeHtml(newPost.score)+'</span></div>'+
        '</td></tr><tr><td>' +
        '<span class="date" style="margin-left:10px;">' + moment(newPost.postedAt).fromNow() + 
        '</td></tr>'+
        '<tr><td></span><p style="margin-left:10px;">' + escapeHtml(newPost.message)  + '</p></td></tr></table></div></div>');
    });
    

    $("#comment").submit(function() {
      var a = postRef.push();
      a.set({
        name: $("#name").val(),
        predicted: $("#res").val().split(",")[0],
        score: $("#res").val().split(",")[1],
        message: $("#message").val(),
        labeled: $("input:radio[name='customRadio']:checked").val(),
        md5Hash: md5($("#message").val()),
        postedAt: Firebase.ServerValue.TIMESTAMP
      });
              
      $("input[type=text], textarea").val("");
      return false;
    });
});

function slugify(text) {
  return text.toString().toLowerCase().trim()
    .replace(/&/g, '-and-')
    .replace(/[\s\W-]+/g, '-')
    .replace(/[^a-zA-Z0-9-_]+/g,'');
}

function escapeHtml(str) {
    var div = document.createElement('div');
    div.appendChild(document.createTextNode(str));
    return div.innerHTML;
}