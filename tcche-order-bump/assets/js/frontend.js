(function ($) {
    'use strict';

    // Set session cookie for analytics
    if (!getCookie('tcche_ob_sid')) {
        var sid = 'ob_' + Math.random().toString(36).substr(2, 12) + '_' + Date.now();
        document.cookie = 'tcche_ob_sid=' + sid + ';path=/;max-age=3600;SameSite=Lax';
    }

    function getCookie(name) {
        var match = document.cookie.match(new RegExp('(^| )' + name + '=([^;]+)'));
        return match ? match[2] : null;
    }

    // Handle checkbox toggle
    $(document).on('change', '.tcche-ob-offer__checkbox', function () {
        var $offer = $(this).closest('.tcche-ob-offer');
        var bumpId = $(this).data('bump-id');
        var isChecked = $(this).is(':checked');

        $offer.find('.tcche-ob-offer__loading').show();

        $.post(tccheOBFront.ajax_url, {
            action: isChecked ? 'tcche_ob_accept_bump' : 'tcche_ob_remove_bump',
            nonce: tccheOBFront.nonce,
            bump_id: bumpId,
        }, function (res) {
            $offer.find('.tcche-ob-offer__loading').hide();

            if (res.success) {
                $offer.toggleClass('tcche-ob-offer--accepted', isChecked);

                // Refresh checkout totals
                $(document.body).trigger('update_checkout');
            } else {
                // Revert checkbox on failure
                $offer.find('.tcche-ob-offer__checkbox').prop('checked', !isChecked);
            }
        }).fail(function () {
            $offer.find('.tcche-ob-offer__loading').hide();
            $offer.find('.tcche-ob-offer__checkbox').prop('checked', !isChecked);
        });
    });

})(jQuery);
