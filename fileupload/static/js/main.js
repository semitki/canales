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

  setType: (file_id) => {
    console.log(file_id);
    console.log(dominator.files.has(file_id));
  },

  add: (e, data) => {
    console.log(e);
    console.log(data);
    console.log($('select#'+data.files[0].name.split('.')[0]));
    if(!dominator.files.has(data.files[0].name.split('.')[0])) {
      console.log('inexistente');
      dominator.files.set(data.files[0].name.split('.')[0],
        {
          type: 'pat'
        }
      );
      $('select#' + data.files[0].name.split('.')[0]).on('change', e => {
        dominator.setType(e.currentTarget.value);
      });
    } else {
      console.log('existe');
      data.abort();
    }
  },

  uploadTemplate: function(o) {
    let rows = $();
    $.each(o.files, function(index, file) {
      let row = $('<tr class="template-upload fade">' +
        '<td><span class="preview"></span></td>' +
        '<td><p class="name"></p>' +
        '<div class="error"></div>' +
        '</td>' +
        '<td><p class="size"></p>' +
        '<div class="progress"></div>' +
        '</td>' +
        '<td>' +
        (!index && !o.options.autoUpload ?
          '<button class="start" disabled>Start</button>' : '') +
        (!index ? '<button class="cancel">Cancel</button>' : '') +
        '</td>' +
        '</tr>'
      );
      row.find('.name').text(file.name);
      row.find('.size').text(o.formatFileSize(file.size));
      if(file.error) {
        row.find('.error').text(file.error);
      }
      rows.add(row);
    });
    return rows;
  }
}

$(function () {
    'use strict';

  dominator.init(); // Initialize the dominator

    // Initialize the jQuery File Upload widget:
    $('#fileupload').fileupload({
        // Uncomment the following to send cross-domain cookies:
        //xhrFields: {withCredentials: true},
        //url: 'server/php/'
      uploadTemplate: dominator.uploadTemplate
    }).bind('fileuploadadded', dominator.add);

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
