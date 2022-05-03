import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { Posts7Component } from './posts7.component';

describe('Posts7Component', () => {
  let component: Posts7Component;
  let fixture: ComponentFixture<Posts7Component>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ Posts7Component ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(Posts7Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
