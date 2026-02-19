<?php defined('ABSPATH') || exit; ?>
<div class="wrap tcche-ob-admin">
    <h1 class="wp-heading-inline"><?php esc_html_e('Order Bumps', 'tcche-order-bump'); ?></h1>
    <a href="<?php echo esc_url(admin_url('admin.php?page=tcche-order-bump-new')); ?>" class="page-title-action">
        <?php esc_html_e('Add New', 'tcche-order-bump'); ?>
    </a>
    <hr class="wp-header-end">

    <?php if (empty($bumps)) : ?>
        <div class="tcche-ob-empty-state">
            <div class="tcche-ob-empty-state__icon">
                <span class="dashicons dashicons-arrow-up-alt" style="font-size:48px;width:48px;height:48px;"></span>
            </div>
            <h2><?php esc_html_e('No Order Bumps Yet', 'tcche-order-bump'); ?></h2>
            <p><?php esc_html_e('Create your first order bump to start increasing your average order value.', 'tcche-order-bump'); ?></p>
            <a href="<?php echo esc_url(admin_url('admin.php?page=tcche-order-bump-new')); ?>" class="button button-primary button-hero">
                <?php esc_html_e('Create Order Bump', 'tcche-order-bump'); ?>
            </a>
        </div>
    <?php else : ?>
        <table class="wp-list-table widefat fixed striped tcche-ob-table">
            <thead>
                <tr>
                    <th class="column-title"><?php esc_html_e('Title', 'tcche-order-bump'); ?></th>
                    <th class="column-product"><?php esc_html_e('Bump Product', 'tcche-order-bump'); ?></th>
                    <th class="column-price"><?php esc_html_e('Price', 'tcche-order-bump'); ?></th>
                    <th class="column-discount"><?php esc_html_e('Discount', 'tcche-order-bump'); ?></th>
                    <th class="column-position"><?php esc_html_e('Position', 'tcche-order-bump'); ?></th>
                    <th class="column-status"><?php esc_html_e('Status', 'tcche-order-bump'); ?></th>
                    <th class="column-actions"><?php esc_html_e('Actions', 'tcche-order-bump'); ?></th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($bumps as $bump) : ?>
                    <tr data-bump-id="<?php echo esc_attr($bump['id']); ?>">
                        <td class="column-title">
                            <strong>
                                <a href="<?php echo esc_url(admin_url('admin.php?page=tcche-order-bump-edit&bump_id=' . $bump['id'])); ?>">
                                    <?php echo esc_html($bump['title']); ?>
                                </a>
                            </strong>
                        </td>
                        <td class="column-product">
                            <?php if ($bump['bump_product_image']) : ?>
                                <img src="<?php echo esc_url($bump['bump_product_image']); ?>"
                                     alt="" style="width:32px;height:32px;vertical-align:middle;margin-right:8px;border-radius:4px;">
                            <?php endif; ?>
                            <?php echo esc_html($bump['bump_product_name']); ?>
                        </td>
                        <td class="column-price">
                            <?php if ($bump['discount_type'] !== 'none' && $bump['bump_price'] < $bump['original_price']) : ?>
                                <del><?php echo wc_price($bump['original_price']); ?></del>
                                <?php echo wc_price($bump['bump_price']); ?>
                            <?php else : ?>
                                <?php echo wc_price($bump['original_price']); ?>
                            <?php endif; ?>
                        </td>
                        <td class="column-discount">
                            <?php
                            if ($bump['discount_type'] === 'percentage') {
                                echo esc_html($bump['discount_value'] . '%');
                            } elseif ($bump['discount_type'] === 'fixed') {
                                echo wc_price($bump['discount_value']);
                            } else {
                                esc_html_e('None', 'tcche-order-bump');
                            }
                            ?>
                        </td>
                        <td class="column-position">
                            <?php
                            $positions = [
                                'after_order_review'     => __('After Order Review', 'tcche-order-bump'),
                                'before_payment'         => __('Before Payment', 'tcche-order-bump'),
                                'after_customer_details' => __('After Customer Details', 'tcche-order-bump'),
                                'after_place_order'      => __('After Place Order', 'tcche-order-bump'),
                            ];
                            echo esc_html($positions[$bump['position']] ?? $bump['position']);
                            ?>
                        </td>
                        <td class="column-status">
                            <span class="tcche-ob-status tcche-ob-status--<?php echo esc_attr($bump['status']); ?>">
                                <?php echo esc_html($bump['status'] === 'publish' ? __('Active', 'tcche-order-bump') : __('Draft', 'tcche-order-bump')); ?>
                            </span>
                        </td>
                        <td class="column-actions">
                            <a href="<?php echo esc_url(admin_url('admin.php?page=tcche-order-bump-edit&bump_id=' . $bump['id'])); ?>"
                               class="button button-small">
                                <?php esc_html_e('Edit', 'tcche-order-bump'); ?>
                            </a>
                            <button type="button"
                                    class="button button-small button-link-delete tcche-ob-delete-btn"
                                    data-bump-id="<?php echo esc_attr($bump['id']); ?>">
                                <?php esc_html_e('Delete', 'tcche-order-bump'); ?>
                            </button>
                        </td>
                    </tr>
                <?php endforeach; ?>
            </tbody>
        </table>
    <?php endif; ?>
</div>
