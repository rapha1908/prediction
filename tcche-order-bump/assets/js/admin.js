(function ($) {
    'use strict';

    function showToast(msg, type) {
        var $t = $('<div class="tcche-ob-toast tcche-ob-toast--' + type + '">' + msg + '</div>');
        $('body').append($t);
        setTimeout(function () { $t.css('opacity', 0); }, 2500);
        setTimeout(function () { $t.remove(); }, 3000);
    }

    /* ---- Product Search ---- */
    var searchTimer;

    function initProductSearch(inputSel, resultsSel, onSelect) {
        var $input = $(inputSel);
        var $results = $(resultsSel);
        if (!$input.length) return;

        $input.on('input', function () {
            clearTimeout(searchTimer);
            var val = $.trim($input.val());
            if (val.length < 2) { $results.hide().empty(); return; }

            searchTimer = setTimeout(function () {
                $.ajax({
                    url: tccheOB.ajax_url,
                    data: { action: 'tcche_ob_search_products', nonce: tccheOB.nonce, term: val },
                    success: function (res) {
                        $results.empty();
                        if (res.success && res.data.length) {
                            $.each(res.data, function (i, p) {
                                $results.append(
                                    '<div class="tcche-ob-search-results__item" data-id="' + p.id + '" data-name="' + p.name + '">' + p.text + '</div>'
                                );
                            });
                            $results.show();
                        } else {
                            $results.hide();
                        }
                    }
                });
            }, 300);
        });

        $(document).on('click', resultsSel + ' .tcche-ob-search-results__item', function () {
            var id = $(this).data('id');
            var name = $(this).data('name');
            onSelect(id, name);
            $input.val('');
            $results.hide().empty();
        });

        $(document).on('click', function (e) {
            if (!$(e.target).closest(inputSel + ',' + resultsSel).length) {
                $results.hide();
            }
        });
    }

    // Bump product selector
    initProductSearch('#bump-product-search', '#bump-product-results', function (id, name) {
        $('#bump-product-id').val(id);
        $('#bump-product-selected').html('Selected: <strong>' + name + '</strong> (#' + id + ')');
    });

    // Trigger product selector
    initProductSearch('#trigger-product-search', '#trigger-product-results', function (id, name) {
        if ($('#trigger-products-list').find('[data-id="' + id + '"]').length) return;
        $('#trigger-products-list').append(
            '<span class="tcche-ob-tag" data-id="' + id + '">' +
            name +
            '<input type="hidden" name="trigger_product_ids[]" value="' + id + '">' +
            '<button type="button" class="tcche-ob-tag__remove">&times;</button>' +
            '</span>'
        );
    });

    $(document).on('click', '.tcche-ob-tag__remove', function () {
        $(this).closest('.tcche-ob-tag').remove();
    });

    /* ---- Discount toggle ---- */
    $('#bump-discount-type').on('change', function () {
        $('#discount-value-row').toggle($(this).val() !== 'none');
    });

    /* ---- Save Bump ---- */
    $('#tcche-ob-bump-form').on('submit', function (e) {
        e.preventDefault();
        var $btn = $('#tcche-ob-save-btn');
        $btn.addClass('is-busy').text(tccheOB.i18n.saving);

        $.post(tccheOB.ajax_url, {
            action: 'tcche_ob_save_bump',
            nonce: tccheOB.nonce,
            bump_id: $('[name="bump_id"]').val(),
            title: $('[name="title"]').val(),
            bump_product_id: $('[name="bump_product_id"]').val(),
            trigger_product_ids: $('[name="trigger_product_ids[]"]').map(function () { return this.value; }).get(),
            trigger_category_ids: $('[name="trigger_category_ids[]"]:checked').map(function () { return this.value; }).get(),
            discount_type: $('[name="discount_type"]').val(),
            discount_value: $('[name="discount_value"]').val(),
            headline: $('[name="headline"]').val(),
            description: $('[name="description"]').val(),
            position: $('[name="position"]').val(),
            priority: $('[name="priority"]').val(),
            status: $('[name="status"]').val(),
        }, function (res) {
            $btn.removeClass('is-busy');
            if (res.success) {
                showToast(tccheOB.i18n.saved, 'success');
                $btn.text(res.data.bump_id ? 'Update' : 'Create');
                if (!$('[name="bump_id"]').val()) {
                    $('[name="bump_id"]').val(res.data.bump_id);
                    $btn.text('Update');
                }
            } else {
                showToast(res.data.message || tccheOB.i18n.error, 'error');
                $btn.text('Save');
            }
        });
    });

    /* ---- Delete Bump ---- */
    $(document).on('click', '.tcche-ob-delete-btn', function () {
        if (!confirm(tccheOB.i18n.confirm_delete)) return;
        var bumpId = $(this).data('bump-id');

        $.post(tccheOB.ajax_url, {
            action: 'tcche_ob_delete_bump',
            nonce: tccheOB.nonce,
            bump_id: bumpId,
        }, function (res) {
            if (res.success) {
                showToast(res.data.message, 'success');
                $('tr[data-bump-id="' + bumpId + '"]').fadeOut(300, function () { $(this).remove(); });
                if (window.location.search.indexOf('tcche-order-bump-edit') !== -1) {
                    window.location.href = 'admin.php?page=tcche-order-bumps';
                }
            } else {
                showToast(res.data.message || tccheOB.i18n.error, 'error');
            }
        });
    });

    /* ---- Analytics ---- */
    var dailyChart = null;
    var revenueChart = null;

    function loadAnalytics() {
        var dateFrom = $('#tcche-ob-date-from').val();
        var dateTo = $('#tcche-ob-date-to').val();

        $.ajax({
            url: tccheOB.ajax_url,
            data: {
                action: 'tcche_ob_get_analytics',
                nonce: tccheOB.nonce,
                date_from: dateFrom,
                date_to: dateTo,
            },
            success: function (res) {
                if (!res.success) return;
                var d = res.data;

                $('#stat-impressions').text(d.summary.impressions.toLocaleString());
                $('#stat-conversions').text(d.summary.conversions.toLocaleString());
                $('#stat-rate').text(d.summary.conversion_rate + '%');
                $('#stat-revenue').text(formatCurrency(d.summary.total_revenue));
                $('#stat-aov').text(formatCurrency(d.summary.avg_order_value));

                renderDailyChart(d.daily);
                renderRevenueChart(d.daily);
                renderBumpTable(d.by_bump);
            }
        });
    }

    function formatCurrency(val) {
        return '$' + parseFloat(val).toFixed(2).replace(/\d(?=(\d{3})+\.)/g, '$&,');
    }

    function renderDailyChart(daily) {
        var ctx = document.getElementById('tcche-ob-daily-chart');
        if (!ctx) return;

        var labels = daily.map(function (r) { return r.date; });
        var imps = daily.map(function (r) { return r.impressions; });
        var convs = daily.map(function (r) { return r.conversions; });

        if (dailyChart) dailyChart.destroy();
        dailyChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [
                    { label: 'Impressions', data: imps, borderColor: '#2271b1', backgroundColor: 'rgba(34,113,177,0.08)', fill: true, tension: 0.3 },
                    { label: 'Conversions', data: convs, borderColor: '#00a32a', backgroundColor: 'rgba(0,163,42,0.08)', fill: true, tension: 0.3 },
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { position: 'top' } },
                scales: {
                    y: { beginAtZero: true, ticks: { stepSize: 1 } },
                    x: { ticks: { maxTicksLimit: 15 } }
                }
            }
        });
    }

    function renderRevenueChart(daily) {
        var ctx = document.getElementById('tcche-ob-revenue-chart');
        if (!ctx) return;

        var labels = daily.map(function (r) { return r.date; });
        var revs = daily.map(function (r) { return parseFloat(r.revenue); });

        if (revenueChart) revenueChart.destroy();
        revenueChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [
                    { label: 'Revenue', data: revs, backgroundColor: 'rgba(34,113,177,0.6)', borderColor: '#2271b1', borderWidth: 1 },
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { position: 'top' } },
                scales: {
                    y: { beginAtZero: true },
                    x: { ticks: { maxTicksLimit: 15 } }
                }
            }
        });
    }

    function renderBumpTable(byBump) {
        var $body = $('#tcche-ob-bump-table-body');
        if (!$body.length) return;
        $body.empty();

        if (!byBump.length) {
            $body.html('<tr><td colspan="6" style="text-align:center;padding:20px;">No data available.</td></tr>');
            return;
        }

        $.each(byBump, function (i, row) {
            $body.append(
                '<tr>' +
                '<td>' + row.bump.title + '</td>' +
                '<td>' + row.bump.bump_product_name + '</td>' +
                '<td>' + row.impressions.toLocaleString() + '</td>' +
                '<td>' + row.conversions.toLocaleString() + '</td>' +
                '<td>' + row.conversion_rate + '%</td>' +
                '<td>' + formatCurrency(row.total_revenue) + '</td>' +
                '</tr>'
            );
        });
    }

    /* Quick date buttons */
    $(document).on('click', '.tcche-ob-quick-date', function () {
        var days = $(this).data('days');
        var to = new Date();
        var from = new Date();
        from.setDate(to.getDate() - days);

        $('#tcche-ob-date-from').val(from.toISOString().split('T')[0]);
        $('#tcche-ob-date-to').val(to.toISOString().split('T')[0]);
        $('.tcche-ob-quick-date').removeClass('active');
        $(this).addClass('active');
    });

    $('#tcche-ob-apply-filter').on('click', loadAnalytics);

    // Auto-load analytics if on analytics page
    if ($('.tcche-ob-analytics').length) {
        $(document).ready(function () {
            if (typeof Chart !== 'undefined') {
                loadAnalytics();
            } else {
                var checkChart = setInterval(function () {
                    if (typeof Chart !== 'undefined') {
                        clearInterval(checkChart);
                        loadAnalytics();
                    }
                }, 200);
            }
        });
    }

})(jQuery);
