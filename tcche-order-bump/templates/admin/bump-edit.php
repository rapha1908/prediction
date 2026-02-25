<?php defined('ABSPATH') || exit; ?>
<div class="wrap tcche-ob-admin">
    <h1>
        <?php echo $bump ? esc_html__('Edit Order Bump', 'tcche-order-bump') : esc_html__('Add New Order Bump', 'tcche-order-bump'); ?>
    </h1>

    <form id="tcche-ob-bump-form" class="tcche-ob-form" method="post">
        <input type="hidden" name="bump_id" value="<?php echo esc_attr($bump ? $bump['id'] : 0); ?>">

        <div class="tcche-ob-form__grid">
            <!-- Main Settings -->
            <div class="tcche-ob-form__main">
                <div class="tcche-ob-card">
                    <h2 class="tcche-ob-card__title"><?php esc_html_e('General Settings', 'tcche-order-bump'); ?></h2>

                    <table class="form-table">
                        <tr>
                            <th><label for="bump-title"><?php esc_html_e('Title (internal)', 'tcche-order-bump'); ?></label></th>
                            <td>
                                <input type="text" id="bump-title" name="title" class="regular-text"
                                       value="<?php echo esc_attr($bump ? $bump['title'] : ''); ?>"
                                       placeholder="<?php esc_attr_e('e.g. Upsell Premium Add-on', 'tcche-order-bump'); ?>" required>
                            </td>
                        </tr>
                        <tr>
                            <th><label for="bump-product"><?php esc_html_e('Bump Product', 'tcche-order-bump'); ?></label></th>
                            <td>
                                <input type="text" id="bump-product-search" class="regular-text tcche-ob-product-search"
                                       placeholder="<?php esc_attr_e('Search products...', 'tcche-order-bump'); ?>"
                                       autocomplete="off">
                                <input type="hidden" id="bump-product-id" name="bump_product_id"
                                       value="<?php echo esc_attr($bump ? $bump['bump_product_id'] : ''); ?>">
                                <div id="bump-product-results" class="tcche-ob-search-results" style="display:none;"></div>
                                <?php if ($bump && $bump['bump_product_name']) : ?>
                                    <p class="description" id="bump-product-selected">
                                        <?php printf(__('Selected: <strong>%s</strong> (#%d)', 'tcche-order-bump'), esc_html($bump['bump_product_name']), $bump['bump_product_id']); ?>
                                    </p>
                                <?php else : ?>
                                    <p class="description" id="bump-product-selected"></p>
                                <?php endif; ?>
                            </td>
                        </tr>
                        <tr>
                            <th><label for="bump-headline"><?php esc_html_e('Headline', 'tcche-order-bump'); ?></label></th>
                            <td>
                                <textarea id="bump-headline" name="headline" class="large-text" rows="2"
                                       placeholder="<?php esc_attr_e('Special Offer!', 'tcche-order-bump'); ?>"><?php echo esc_textarea($bump ? $bump['headline'] : 'Special Offer!'); ?></textarea>
                                <p class="description"><?php esc_html_e('Shown as the bump offer title on checkout. Use line breaks for multi-line headlines.', 'tcche-order-bump'); ?></p>
                            </td>
                        </tr>
                        <tr>
                            <th><label for="bump-description"><?php esc_html_e('Description', 'tcche-order-bump'); ?></label></th>
                            <td>
                                <textarea id="bump-description" name="description" class="large-text" rows="4"><?php echo esc_textarea($bump ? $bump['description'] : ''); ?></textarea>
                                <p class="description"><?php esc_html_e('Line breaks will be preserved on checkout.', 'tcche-order-bump'); ?></p>
                            </td>
                        </tr>
                    </table>
                </div>

                <div class="tcche-ob-card">
                    <h2 class="tcche-ob-card__title"><?php esc_html_e('Discount', 'tcche-order-bump'); ?></h2>

                    <table class="form-table">
                        <tr>
                            <th><label for="bump-discount-type"><?php esc_html_e('Discount Type', 'tcche-order-bump'); ?></label></th>
                            <td>
                                <select id="bump-discount-type" name="discount_type">
                                    <option value="none" <?php selected($bump ? $bump['discount_type'] : '', 'none'); ?>><?php esc_html_e('No Discount', 'tcche-order-bump'); ?></option>
                                    <option value="percentage" <?php selected($bump ? $bump['discount_type'] : '', 'percentage'); ?>><?php esc_html_e('Percentage', 'tcche-order-bump'); ?></option>
                                    <option value="fixed" <?php selected($bump ? $bump['discount_type'] : '', 'fixed'); ?>><?php esc_html_e('Fixed Amount', 'tcche-order-bump'); ?></option>
                                </select>
                            </td>
                        </tr>
                        <tr id="discount-value-row" style="<?php echo ($bump && $bump['discount_type'] !== 'none') ? '' : 'display:none;'; ?>">
                            <th><label for="bump-discount-value"><?php esc_html_e('Discount Value', 'tcche-order-bump'); ?></label></th>
                            <td>
                                <input type="number" id="bump-discount-value" name="discount_value"
                                       value="<?php echo esc_attr($bump ? $bump['discount_value'] : 0); ?>"
                                       min="0" step="0.01" class="small-text">
                            </td>
                        </tr>
                    </table>
                </div>

                <div class="tcche-ob-card">
                    <h2 class="tcche-ob-card__title"><?php esc_html_e('Trigger Conditions', 'tcche-order-bump'); ?></h2>
                    <p class="description"><?php esc_html_e('Leave empty to show the bump to all customers. Otherwise, the bump shows only when cart contains matching products or categories.', 'tcche-order-bump'); ?></p>

                    <table class="form-table">
                        <tr>
                            <th><label><?php esc_html_e('Trigger Products', 'tcche-order-bump'); ?></label></th>
                            <td>
                                <input type="text" id="trigger-product-search" class="regular-text tcche-ob-product-search"
                                       placeholder="<?php esc_attr_e('Search products to add...', 'tcche-order-bump'); ?>"
                                       autocomplete="off">
                                <div id="trigger-product-results" class="tcche-ob-search-results" style="display:none;"></div>
                                <div id="trigger-products-list" class="tcche-ob-tag-list">
                                    <?php
                                    if ($bump && !empty($bump['trigger_product_ids'])) :
                                        foreach ($bump['trigger_product_ids'] as $pid) :
                                            $p = wc_get_product($pid);
                                            if (!$p) continue;
                                    ?>
                                        <span class="tcche-ob-tag" data-id="<?php echo esc_attr($pid); ?>">
                                            <?php echo esc_html($p->get_name()); ?>
                                            <input type="hidden" name="trigger_product_ids[]" value="<?php echo esc_attr($pid); ?>">
                                            <button type="button" class="tcche-ob-tag__remove">&times;</button>
                                        </span>
                                    <?php endforeach; endif; ?>
                                </div>
                            </td>
                        </tr>
                        <tr>
                            <th><label><?php esc_html_e('Trigger Categories', 'tcche-order-bump'); ?></label></th>
                            <td>
                                <div class="tcche-ob-category-checkboxes">
                                    <?php
                                    $selected_cats = $bump ? (array) $bump['trigger_category_ids'] : [];
                                    if (!empty($categories) && !is_wp_error($categories)) :
                                        foreach ($categories as $cat) :
                                    ?>
                                        <label class="tcche-ob-category-label">
                                            <input type="checkbox" name="trigger_category_ids[]"
                                                   value="<?php echo esc_attr($cat->term_id); ?>"
                                                   <?php checked(in_array($cat->term_id, $selected_cats)); ?>>
                                            <?php echo esc_html($cat->name); ?>
                                        </label>
                                    <?php endforeach; endif; ?>
                                </div>
                            </td>
                        </tr>
                    </table>
                </div>
            </div>

            <!-- Sidebar -->
            <div class="tcche-ob-form__sidebar">
                <div class="tcche-ob-card">
                    <h2 class="tcche-ob-card__title"><?php esc_html_e('Publish', 'tcche-order-bump'); ?></h2>

                    <div class="tcche-ob-card__body">
                        <p>
                            <label for="bump-status"><?php esc_html_e('Status', 'tcche-order-bump'); ?></label>
                            <select id="bump-status" name="status" style="width:100%;">
                                <option value="publish" <?php selected($bump ? $bump['status'] : '', 'publish'); ?>><?php esc_html_e('Active', 'tcche-order-bump'); ?></option>
                                <option value="draft" <?php selected($bump ? $bump['status'] : '', 'draft'); ?>><?php esc_html_e('Draft', 'tcche-order-bump'); ?></option>
                            </select>
                        </p>

                        <p>
                            <label for="bump-position"><?php esc_html_e('Position on Checkout', 'tcche-order-bump'); ?></label>
                            <select id="bump-position" name="position" style="width:100%;">
                                <option value="after_order_review" <?php selected($bump ? $bump['position'] : '', 'after_order_review'); ?>><?php esc_html_e('After Order Review', 'tcche-order-bump'); ?></option>
                                <option value="before_payment" <?php selected($bump ? $bump['position'] : '', 'before_payment'); ?>><?php esc_html_e('Before Payment', 'tcche-order-bump'); ?></option>
                                <option value="after_customer_details" <?php selected($bump ? $bump['position'] : '', 'after_customer_details'); ?>><?php esc_html_e('After Customer Details', 'tcche-order-bump'); ?></option>
                                <option value="after_place_order" <?php selected($bump ? $bump['position'] : '', 'after_place_order'); ?>><?php esc_html_e('After Place Order', 'tcche-order-bump'); ?></option>
                            </select>
                        </p>

                        <p>
                            <label for="bump-design-style"><?php esc_html_e('Design Preset', 'tcche-order-bump'); ?></label>
                            <select id="bump-design-style" name="design_style" style="width:100%;">
                                <option value="classic" <?php selected($bump ? ($bump['design_style'] ?? 'classic') : 'classic', 'classic'); ?>><?php esc_html_e('Classic', 'tcche-order-bump'); ?></option>
                                <option value="minimal" <?php selected($bump ? ($bump['design_style'] ?? '') : '', 'minimal'); ?>><?php esc_html_e('Minimal', 'tcche-order-bump'); ?></option>
                                <option value="bold" <?php selected($bump ? ($bump['design_style'] ?? '') : '', 'bold'); ?>><?php esc_html_e('Bold', 'tcche-order-bump'); ?></option>
                                <option value="rounded" <?php selected($bump ? ($bump['design_style'] ?? '') : '', 'rounded'); ?>><?php esc_html_e('Rounded', 'tcche-order-bump'); ?></option>
                            </select>
                        </p>

                        <p>
                            <label for="bump-priority"><?php esc_html_e('Priority', 'tcche-order-bump'); ?></label>
                            <input type="number" id="bump-priority" name="priority" class="small-text"
                                   value="<?php echo esc_attr($bump ? $bump['priority'] : 10); ?>" min="1" max="100">
                            <span class="description"><?php esc_html_e('Lower = shows first', 'tcche-order-bump'); ?></span>
                        </p>
                    </div>

                    <div class="tcche-ob-card__footer">
                        <?php if ($bump) : ?>
                            <button type="button" class="button button-link-delete tcche-ob-delete-btn"
                                    data-bump-id="<?php echo esc_attr($bump['id']); ?>"
                                    style="float:left;">
                                <?php esc_html_e('Delete', 'tcche-order-bump'); ?>
                            </button>
                        <?php endif; ?>
                        <button type="submit" class="button button-primary" id="tcche-ob-save-btn">
                            <?php echo $bump ? esc_html__('Update', 'tcche-order-bump') : esc_html__('Create', 'tcche-order-bump'); ?>
                        </button>
                    </div>
                </div>
            </div>
        </div>
    </form>
</div>
