$(document).ready(function () {

    $('#btn-submt').click(function () {
        var form_data = new FormData($('#userid')[0]);
        alert(form_data);
        $.ajax({
            type: 'POST',
            url: '/suggest',
            data: form_data,
            contentType: false,
            cache: false,
            processData: false,
            async: false,
            success: function (data) {
                console.log('Success!');
            },
        });     
    });
    
});




