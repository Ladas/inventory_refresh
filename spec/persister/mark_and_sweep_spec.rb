require_relative "test_collector"
require_relative 'targeted_refresh_spec_helper'
require_relative '../helpers/spec_parsed_data'


describe InventoryRefresh::Persister do
  include TargetedRefreshSpecHelper
  include SpecParsedData

  before(:each) do
    @ems = FactoryGirl.create(:ems_container, :name => "test_ems")
  end

  context "with :retention_strategy => 'archive'" do
    it "automatically fills :last_seen_at timestamp for refreshed entities" do
      time_now = Time.now.utc
      time_before = Time.now.utc - 20.seconds
      time_after  = Time.now.utc + 20.seconds

      _cg1 = FactoryGirl.create(:container_group, container_group_data(1).merge(:ext_management_system => @ems, :resource_timestamp => time_before))
      _cg2 = FactoryGirl.create(:container_group, container_group_data(2).merge(:ext_management_system => @ems, :resource_timestamp => time_after))
      _cg3 = FactoryGirl.create(:container_group, container_group_data(3).merge(:ext_management_system => @ems, :resource_timestamp => time_now))
      _cg4 = FactoryGirl.create(:container_group, container_group_data(4).merge(:ext_management_system => @ems, :last_seen_at => time_before))
      _cg6 = FactoryGirl.create(:container_group, container_group_data(6).merge(:ext_management_system => @ems, :last_seen_at => time_before))
      _cg7 = FactoryGirl.create(:container_group, container_group_data(7).merge(:ext_management_system => @ems, :last_seen_at => time_before))

      refresh_state_uuid = SecureRandom.uuid
      part1_uuid = SecureRandom.uuid
      part2_uuid = SecureRandom.uuid

      # Refresh first part and mark :last_seen_at
      persister = create_containers_persister(:retention_strategy => "archive")
      persister.refresh_state_uuid = refresh_state_uuid
      persister.refresh_state_part_uuid = part1_uuid

      persister.container_groups.build(container_group_data(1).merge(:ext_management_system => @ems, :resource_timestamp => time_before))
      persister.container_groups.build(container_group_data(2).merge(:ext_management_system => @ems, :resource_timestamp => time_before))
      persister.container_groups.build(container_group_data(5).merge(:ext_management_system => @ems, :resource_timestamp => time_before))
      persister.persist!

      # We don't update any records data, but last_seen_at is updated for all records involved
      expect(persister.container_groups.updated_records).to(match_array([]))

      date_field = ContainerGroup.arel_table[:last_seen_at]
      expect(ContainerGroup.where(date_field.gt(time_now)).pluck(:ems_ref)).to(
        match_array([container_group_data(1)[:ems_ref], container_group_data(2)[:ems_ref],
                     container_group_data(5)[:ems_ref]])
      )
      expect(ContainerGroup.where(date_field.lt(time_now)).or(ContainerGroup.where(:last_seen_at => nil)).pluck(:ems_ref)).to(
        match_array([container_group_data(3)[:ems_ref], container_group_data(4)[:ems_ref],
                    container_group_data(6)[:ems_ref], container_group_data(7)[:ems_ref]])
      )

      # Refresh second part and mark :last_seen_at
      persister = create_containers_persister(:retention_strategy => "archive")
      persister.refresh_state_uuid = refresh_state_uuid
      persister.refresh_state_part_uuid = part2_uuid

      persister.container_groups.build(container_group_data(6).merge(:ext_management_system => @ems, :resource_timestamp => time_before))
      persister.persist!

      date_field = ContainerGroup.arel_table[:last_seen_at]
      expect(ContainerGroup.where(date_field.gt(time_now)).pluck(:ems_ref)).to(
        match_array([container_group_data(1)[:ems_ref], container_group_data(2)[:ems_ref],
                     container_group_data(5)[:ems_ref], container_group_data(6)[:ems_ref]])
      )
      expect(ContainerGroup.where(date_field.lt(time_now)).or(ContainerGroup.where(:last_seen_at => nil)).pluck(:ems_ref)).to(
        match_array([container_group_data(3)[:ems_ref], container_group_data(4)[:ems_ref],
                     container_group_data(7)[:ems_ref]])
      )

      # Send persister with total_parts = XY, that will cause sweeping all tables having :last_seen_on column
      persister = create_containers_persister(:retention_strategy => "archive")
      persister.refresh_state_uuid = refresh_state_uuid
      persister.total_parts = 2
      persister.persist!

      require 'byebug'; byebug
    end

      # it "archives nested data with all_manager_uuids_timestamp" do
      #   time_now    = Time.now.utc
      #   time_before = Time.now.utc - 20.seconds
      #   time_after  = Time.now.utc + 20.seconds
      #
      #   full_refresh_start = time_now
      #
      #   cg1 = FactoryGirl.create(:container_group, container_group_data(1).merge(:ext_management_system => @ems, :resource_timestamp => time_now))
      #   cg2 = FactoryGirl.create(:container_group, container_group_data(2).merge(:ext_management_system => @ems, :resource_timestamp => time_now))
      #   _c11 = FactoryGirl.create(:nested_container, nested_container_data(11).merge(:container_group => cg1, :resource_timestamp => time_now))
      #   _c12 = FactoryGirl.create(:nested_container, nested_container_data(12).merge(:container_group => cg1, :resource_timestamp => time_now))
      #   _c21 = FactoryGirl.create(:nested_container, nested_container_data(21).merge(:container_group => cg2, :resource_timestamp => time_now))
      #   _c22 = FactoryGirl.create(:nested_container, nested_container_data(22).merge(:container_group => cg2, :resource_timestamp => time_now))
      #
      #   # We are sending older data, that should not cause any archival, but we should create the non existent old data
      #   # nested_container_data(13) and archive them. And we are also sending new data.
      #   persister = create_containers_persister(:retention_strategy => "archive")
      #   persister.container_groups.build(container_group_data(1).merge(:resource_timestamp => time_before))
      #   persister.nested_containers.build(
      #     nested_container_data(11).merge(
      #       :container_group    => persister.container_groups.lazy_find(container_group_data(1)[:ems_ref]),
      #       :resource_timestamp => time_before
      #     )
      #   )
      #   persister.nested_containers.build(
      #     nested_container_data(13).merge(
      #       :container_group    => persister.container_groups.lazy_find(container_group_data(1)[:ems_ref]),
      #       :resource_timestamp => time_before
      #     )
      #   )
      #   persister.container_groups.build(container_group_data(2).merge(:resource_timestamp => time_before))
      #   persister.nested_containers.build(
      #     nested_container_data(21).merge(
      #       :container_group    => persister.container_groups.lazy_find(container_group_data(2)[:ems_ref]),
      #       :resource_timestamp => time_after
      #     )
      #   )
      #   persister.nested_containers.build(
      #     nested_container_data(23).merge(
      #       :container_group    => persister.container_groups.lazy_find(container_group_data(2)[:ems_ref]),
      #       :resource_timestamp => time_after
      #     )
      #   )
      #
      #   persister.persist!
      #
      #   expect(ContainerGroup.active.pluck(:ems_ref)).to(
      #     match_array([container_group_data(1)[:ems_ref], container_group_data(2)[:ems_ref]])
      #   )
      #   expect(ContainerGroup.archived.pluck(:ems_ref)).to(
      #     match_array([])
      #   )
      #
      #   expect(NestedContainer.active.pluck(:ems_ref)).to(
      #     match_array([nested_container_data(11)[:ems_ref], nested_container_data(13)[:ems_ref],
      #                  nested_container_data(21)[:ems_ref], nested_container_data(23)[:ems_ref]])
      #   )
      #   expect(NestedContainer.archived.pluck(:ems_ref)).to(
      #     match_array([nested_container_data(12)[:ems_ref], nested_container_data(22)[:ems_ref]])
      #   )
      #   # TODO(lsmola) This should be the right thing, but there is no way to enforce ensure this now, we test that next
      #   # refresh will fix it, archiving nested container 13 and reconnecting 12.
      #   #
      #   # expect(NestedContainer.active.pluck(:ems_ref)).to(
      #   #   match_array([nested_container_data(11)[:ems_ref], nested_container_data(12)[:ems_ref],
      #   #                nested_container_data(21)[:ems_ref], nested_container_data(23)[:ems_ref]])
      #   # )
      #   # expect(NestedContainer.archived.pluck(:ems_ref)).to(
      #   #   match_array([nested_container_data(13)[:ems_ref], , nested_container_data(22)[:ems_ref]])
      #   # )
      #
      #   # We are sending newer data
      #   persister = create_containers_persister(:retention_strategy => "archive")
      #   persister.container_groups.build(container_group_data(1).merge(:resource_timestamp => time_after))
      #   persister.nested_containers.build(
      #     nested_container_data(11).merge(
      #       :container_group    => persister.container_groups.lazy_find(container_group_data(1)[:ems_ref]),
      #       :resource_timestamp => time_after
      #     )
      #   )
      #   persister.nested_containers.build(
      #     nested_container_data(12).merge(
      #       :container_group    => persister.container_groups.lazy_find(container_group_data(1)[:ems_ref]),
      #       :resource_timestamp => time_after
      #     )
      #   )
      #
      #   persister.persist!
      #
      #   expect(ContainerGroup.active.pluck(:ems_ref)).to(
      #     match_array([container_group_data(1)[:ems_ref], container_group_data(2)[:ems_ref]])
      #   )
      #   expect(ContainerGroup.archived.pluck(:ems_ref)).to(
      #     match_array([])
      #   )
      #
      #   expect(NestedContainer.active.pluck(:ems_ref)).to(
      #     match_array([nested_container_data(11)[:ems_ref], nested_container_data(12)[:ems_ref],
      #                  nested_container_data(21)[:ems_ref], nested_container_data(23)[:ems_ref]])
      #   )
      #   expect(NestedContainer.archived.pluck(:ems_ref)).to(
      #     match_array([nested_container_data(13)[:ems_ref], nested_container_data(22)[:ems_ref]])
      #   )
      # end
  end
end
