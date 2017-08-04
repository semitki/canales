/*
 * jQuery File Upload Plugin JS Example 8.8.2
 * https://github.com/blueimp/jQuery-File-Upload
 *
 * Copyright 2010, Sebastian Tschan
 * https://blueimp.net
 *
 * Licensed under the MIT license:
 * http://www.opensource.org/licenses/MIT
 */

/*jslint nomen: true, regexp: true */
/*global $, window, blueimp */

'use strict';

let dominator = {

  init: function() {
    this.files = new Map();
  },

  add: function(e, data) {
    console.log(e);
    console.log(data);
    if(!dominator.files.has(data.files[0].name)) {
      console.log('inexistente');
      dominator.files.set(data.files[0].name,
        {
          type: 'pat'
        }
      );
    } else {
      console.log('existe');
      data.abort();
    }
  },


}

$(function () {
    'use strict';

  dominator.init(); // Initialize the dominator

    // Initialize the jQuery File Upload widget:
    $('#fileupload').fileupload({
        // Uncomment the following to send cross-domain cookies:
        //xhrFields: {withCredentials: true},
        //url: 'server/php/'
    }).bind('fileuploadadd', dominator.add);

    // Enable iframe cross-domain access via redirect option:
    $('#fileupload').fileupload(
        'option',
        'redirect',
        window.location.href.replace(
            /\/[^\/]*$/,
            '/cors/result.html?%s'
        )
    );

    // Load existing files:
    $('#fileupload').addClass('fileupload-processing');
    $.ajax({
        // Uncomment the following to send cross-domain cookies:
        //xhrFields: {withCredentials: true},
        //url: $('#fileupload').fileupload('option', 'url'),
        url: '/upload/view/',
        dataType: 'json',
        context: $('#fileupload')[0]
    }).always(function () {
        $(this).removeClass('fileupload-processing');
    }).done(function (result) {
        $(this).fileupload('option', 'done')
            .call(this, null, {result: result});
    });

});
